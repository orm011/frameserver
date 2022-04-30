from genericpath import exists
import av
import pandas as pd
from collections import deque
from tqdm.auto import tqdm

def get_frame_info(path, use_tqdm=False):
    frame_info = deque()
    packet_info = deque()

    #for basic api https://pyav.org/docs/develop/cookbook/basics.html
    with av.open(path) as container:
        stream = container.streams.video[0]
        frames = stream.frames
        with tqdm(total=stream.frames) as pbar:
            frame_no = 0
            for packet_no, packet in enumerate(container.demux(stream)):
                if packet.is_keyframe:
                    keyframe_packet = packet_no
            
                if packet.pos:
                    pos = packet.pos # implies it will re-use previous pos when none
                    ## assumes first pos exists...
                
                packet_info.append({'packet_no':packet_no, 
                                    'is_keyframe':packet.is_keyframe,
                                    'keyframe_packet_no':keyframe_packet,
                                    'packet_pos':pos,
                                    'packet_size':packet.size})
            
                num_frames = 0
                for frame in packet.decode():
                    frame_info.append({
                                'frame_no':frame_no,
                                'frame_index':frame.index, # saving it but seems unreliable
                                'frame_picture_type':frame.pict_type.name, 
                                'packet_no':packet_no,
                                'packet_frame_index':num_frames,
                                'time_s':frame.time,
                                })
                    frame_no +=1
                    num_frames +=1

                packet_info[-1]['num_frames'] = num_frames
                pbar.update()

    frame_df = pd.DataFrame(frame_info)
    return pd.DataFrame(packet_info), frame_df

import os
def _get_cache_path(video_path):
    cache_root = os.environ.get('FRAMESERVER_CACHE', None)
    if cache_root is None:
        assert os.environ.get("HOME")
        cache_root = f'{os.environ.get("HOME")}/.cache/frameserver'
        
    cache_root = os.path.realpath(cache_root)
    os.makedirs(cache_root, exist_ok=True)

    video_path = os.path.realpath(os.path.expanduser(video_path))
    fullpath =  os.path.normpath(f'{cache_root}/{video_path}')

    os.makedirs(fullpath, exist_ok=True)
    return fullpath


import PIL.Image
import skvideo.io
import sys
import json

_rotation_map = {'90':PIL.Image.Transpose.ROTATE_90,
                '270':PIL.Image.Transpose.ROTATE_270,
                '180':PIL.Image.Transpose.ROTATE_180,
                '0':None,
                }

class FFProbe:
    def __init__(self, ffprobe):
        self.ffprobe = ffprobe if ffprobe else {}

    @staticmethod
    def get(path):
        ffprobe = skvideo.io.ffprobe(path)
        return FFProbe(ffprobe)

    def get_rotation(self):
        rot =  (self.ffprobe.get('video', {})
            .get('side_data_list', {})
            .get('side_data', {})
            .get('@rotation', None))

        return _rotation_map.get(rot, None)

    def get_frame_size(self):
        x = self.ffprobe
        width = int(x.get('video',{}).get('@width', None))
        height = int(x.get('video',{}).get('@height', None))

        rot = self.get_rotation()
        if rot in [PIL.Image.Transpose.ROTATE_90, PIL.Image.Transpose.ROTATE_270]:
            tmp = width
            width = height
            height = tmp
        
        return (width, height)
    
class VideoFrameIndex:
    def __init__(self, packet_df, frame_df):
        self.packet_df = packet_df
        self.frame_df = frame_df

    @staticmethod
    def _from_video_file(path):
        packet_df, frame_df = get_frame_info(path)
        return VideoFrameIndex(packet_df, frame_df)

    @staticmethod
    def get_index(video_path, invalidate=False): ## main entry point. will compute index if not done before
        cpath = _get_cache_path(video_path)

        def _compute():
            index = VideoFrameIndex._from_video_file(video_path)
            index.save(cpath)
            return index

        if invalidate:
            return _compute()

        try:
            return VideoFrameIndex.load(cpath)
        except:
            pass

        return _compute()
        
    @staticmethod
    def load(cache_path):
        packet_df = pd.read_parquet(f'{cache_path}/packet_df.parquet')
        frame_df = pd.read_parquet(f'{cache_path}/frame_df.parquet')
        return VideoFrameIndex(packet_df, frame_df)

    def save(self, save_path):
        os.makedirs(save_path, exist_ok=True)
        self.packet_df.to_parquet(f'{save_path}/packet_df.parquet')
        self.frame_df.to_parquet(f'{save_path}/frame_df.parquet')

    def get_packet_coordinates(self, frame_index):
        ent = self.frame_df[['packet_no', 'packet_frame_index']].iloc[frame_index]
        key_no = self.packet_df.keyframe_packet_no.iloc[ent.packet_no]
        return {'key_packet_idx':int(key_no), # packet no where to init codec
                'local_packet_idx':int(ent.packet_no - key_no), # how many packets to decode after
                'local_frame_idx':int(ent.packet_frame_index), # which frame in the packet
               }
    
    def _get_byte_coords(self, packet_no):
        ent = self.packet_df.iloc[packet_no]
        return (int(ent.packet_pos), int(ent.packet_size))
    
    def get_byte_coords(self, packet_coordinates):
        ans = []
        start = packet_coordinates['key_packet_idx']
        for packet_no in range(start, start + packet_coordinates['local_packet_idx'] + 1):
            ans.append(self._get_byte_coords(packet_no))
            
        return ans

