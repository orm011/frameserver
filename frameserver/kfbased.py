from collections import deque
import av
import pandas as pd
import sys
import os
from .index import _get_cache_path

import math

def _get_keyframe_index_part(path_long, start_pts=None, end_pts=None):
    with av.open(path_long) as container:
        stream = container.streams.video[0]
        stream.codec_context.skip_frame = "NONKEY"
        
        if start_pts is not None:
            container.seek(offset=start_pts, backward=True, any_frame=False, stream=stream)
            
        pts = deque()
        for frame in container.decode(stream):
            if end_pts is not None and frame.pts >= end_pts:
                break
            pts.append((int(frame.pts), frame.time))
    
    return pd.DataFrame(pts, columns=['frame_pts', 'time_s'])

def _get_tasks_pts(stream, granularity='5 min'):
    granularity_s = pd.Timedelta(granularity).total_seconds()
    duration_s = float(stream.duration * stream.time_base)
    num_parts = duration_s / granularity_s
    delta = math.ceil(stream.duration/num_parts)
    starts = list(range(0, stream.duration, delta))
    part_list = [stream.start_time + pts for pts in starts]
    assert part_list[-1] + delta >= stream.start_time + stream.duration
    return part_list, delta

def _get_keyframe_index(path_long, pool=None, granularity='5 min'):
    with av.open(path_long, 'r') as container:
        stream = container.streams.video[0]
        if pool: # can use a multiprocess pool or a ray pool
            starts, delta = _get_tasks_pts(stream, granularity=granularity)
            tuples = [(path_long, start_pts, start_pts + delta) for start_pts in starts]
            dfs = pool.starmap(_get_keyframe_index_part, tuples)
            df = pd.concat(dfs, ignore_index=True)
            df = df.drop_duplicates().reset_index(drop=True) # some subtasks may end up overlapping
        else:
            df = _get_keyframe_index_part(path_long, start_pts=None, end_pts=None)
    return df

class FrameNotFoundException(Exception):
    pass

class KeyFrameIndex:
    def __init__(self, keyframe_df):
        self.df = keyframe_df

    @staticmethod
    def _from_video_file(path, pool=None):
        df = _get_keyframe_index(path, pool)
        return KeyFrameIndex(df)

    @staticmethod
    def get(video_path, invalidate=False, pool=None): ## main entry point. will compute index if not done before
        cpath = _get_cache_path(video_path)

        def _compute():
            index = KeyFrameIndex._from_video_file(video_path, pool=None)
            index.save(cpath)
            return index

        if invalidate:
            return _compute()

        try:
            return KeyFrameIndex.load(cpath)
        except:
            pass

        return _compute()
        
    @staticmethod
    def load(cache_path):
        df = pd.read_parquet(f'{cache_path}/keyframe_df.parquet')
        return KeyFrameIndex(df)

    def save(self, save_path):
        os.makedirs(save_path, exist_ok=True)
        self.df.to_parquet(f'{save_path}/keyframe_df.parquet')

    def get_pts(self, keyframe_no):
        if keyframe_no >= self.df.shape[0]:
            raise FrameNotFoundException()
        else:
            return int(self.df.iloc[keyframe_no].frame_pts)

def _get_pts(path, keyframe_no, index=None):
    if index is not None:
        return index.get_pts(keyframe_no)

    with av.open(path, 'r') as container:
        stream = container.streams.video[0]
        stream.codec_context.skip_frame = "NONKEY"

        for (i,frame) in enumerate(container.decode(stream)):
            if i == keyframe_no:
                return int(frame.pts)
            
        raise FrameNotFoundException()
    
def get_frame(path,  *, keyframe_no : int, frame_no: int, 
                        index : KeyFrameIndex = None , 
                        thread_type : str =None):
    key_pts = _get_pts(path, keyframe_no, index=index)        
    with av.open(path, 'r') as container:
        stream = container.streams.video[0]
        if thread_type:
            assert thread_type in ['FRAME', 'AUTO']
            container.streams.video[0].thread_type = thread_type
        container.seek(offset=key_pts, backward=True, any_frame=False, stream=stream)
        for i,frame in enumerate(container.decode(stream)):
            if i == 0:
                assert frame.pts == key_pts
    
            if i == frame_no:
                return frame.to_image()            
        raise FrameNotFoundException()

def _get_frame_reference_impl(path,  *, keyframe_no : int, frame_no: int):
    curr_keyframe = -1
    curr_frame_no = 0
    with av.open(path, 'r') as container:
        stream = container.streams.video[0]
        for frame in container.decode(stream):
            if frame.key_frame and curr_keyframe != keyframe_no:
                curr_keyframe+=1
            
            if curr_keyframe == keyframe_no:
                if curr_frame_no == frame_no:
                    return frame.to_image()

                curr_frame_no+=1
                
        raise FrameNotFoundException()

from .index import FFProbe
import PIL.Image

def get_frame2(path, *, keyframe_no, frame_no, index : KeyFrameIndex = None) -> PIL.Image.Image:
    ffprobe = FFProbe.get(path)
    rotation = ffprobe.get_rotation()
    frm = get_frame(path, keyframe_no=keyframe_no, frame_no=frame_no, index=index)
    if rotation:
        return frm.transpose(method=rotation)
    else:
        return frm