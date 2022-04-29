import av
import pandas as pd
from collections import deque
from tqdm.auto import tqdm

def get_frame_info(path, use_tqdm=False):
    frame_info = deque()
    packet_info = deque()

    #for basic api https://pyav.org/docs/develop/cookbook/basics.html
    with av.open(path) as container:
        frames = container.streams.video[0].frames
        with tqdm(total=frames) as pbar:
            frame_no = 0
            for packet_no, packet in enumerate(container.demux(video=[0])):
                if packet.is_keyframe:
                    keyframe_packet = packet_no

                if packet.pos is None:
                    assert frames - frame_no < 5, 'the last few packets can be dummy ones according to pyav. normally see 2'
                    pbar.update()
                    continue
            
                packet_info.append({'packet_no':packet_no, 
                                    'is_keyframe':packet.is_keyframe,
                                    'keyframe_packet_no':keyframe_packet,
                                    'packet_pos':packet.pos,
                                    'packet_size':packet.size})
            
                num_frames = 0
                for frame in packet.decode():
                    frame_info.append({
                                'frame_no':frame_no,
                                'frame_index':frame.index,
                                'frame_picture_type':frame.pict_type.name, 
                                'packet_no':packet_no,
                                'packet_frame_index':num_frames,
                                'time_s':frame.time,
                                })
                    frame_no +=1
                    num_frames +=1

                packet_info[-1]['num_frames'] = num_frames
                pbar.update()

    frame_df = pd.DataFrame(frame_info).set_index('frame_index').sort_index()
    frame_df.index = frame_df.index.rename('frame_index')
    return pd.DataFrame(packet_info), frame_df

class VideoFrameIndex:
    def __init__(self, packet_df, frame_df):
        self.packet_df = packet_df
        self.frame_df = frame_df

        ## frames are assumed to be indexed by frame.index as returned in a sequential read
        assert self.frame_df.index.is_monotonic_increasing
        
    @staticmethod
    def from_video_file(path):
        packet_df, frame_df = get_frame_info(path)
        return VideoFrameIndex(packet_df, frame_df)

    def get_packet_coordinates(self, frame_index):
        ent = self.frame_df[['packet_no', 'packet_frame_index']].loc[frame_index]
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