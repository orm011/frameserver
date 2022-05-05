import skvideo.io
import PIL.Image

import math
import pandas as pd
import subprocess
import os
import hashlib
import numpy as np

def get_md5(image):
    x = np.array(image)
    if not x.flags['C_CONTIGUOUS']:
        x = np.ascontiguousarray(x)
        
    sh = hashlib.md5(x)
    return sh.hexdigest()

def remux2mp4(input_path, output_dir=None, loglevel='warning'):
    """ copies video content into an .mp4 container with a moov indexing atom, without
        re-encoding. 
        @loglevel: can also use 'info' (see ffmpeg doc)
    """
    file_name = os.path.basename(input_path)
    input_dir = os.path.dirname(input_path)
    if output_dir is None:
        output_dir = f'{input_dir}/.remuxed/'
        
    os.makedirs(output_dir, exist_ok=True)
    output_path = f'{output_dir}/{file_name}.mp4'
    
    subprocess.check_call([
            'ffmpeg',
            '-hide_banner',
            '-loglevel', loglevel, 
            '-y', # overwrite output file if needed
            '-i', input_path,
            '-c', 'copy', # don't re-encode as that is very expensive, just copy to new container
            '-movflags','faststart', # add moov atom to file start so it can be parsed easily
            output_path
    ])
    
    return output_path

def get_pts_intervals(stream, granularity='5 min', num_tasks=None):
    if num_tasks is None:
        granularity_s = pd.Timedelta(granularity).total_seconds()
        duration_s = float(stream.duration * stream.time_base)
        num_parts = duration_s / granularity_s
    else:
        num_parts = num_tasks
        
    delta = math.ceil((stream.duration + 1)/num_parts)
    starts = range(stream.start_time, stream.start_time + stream.duration + 1, delta)
    part_list = [(pts, pts + delta) for pts in starts]
    assert part_list[0][0] == stream.start_time
    assert part_list[-1][-1] > stream.start_time + stream.duration
    return part_list

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


class FixRotationTx:
    def __init__(self, rotation):
        self.rotation = rotation

    def __call__(self, image):
        if self.rotation:
            return image.transpose(method=self.rotation)
        else:
            return image

def get_image_rotation_tx(path):
    ffprobe = FFProbe.get(path)
    rotation = ffprobe.get_rotation()
    return FixRotationTx(rotation)