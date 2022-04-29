import av
import pandas as pd
from collections import deque

def _get_frame_sequential1(path, frame_index):
    """sequentially decode until get to frame
    """
    with av.open(path) as container:
        #stream.thread_type = "AUTO" # faster but not sure it works
        for frame in container.decode(video=0):#demux(video=[0]):
            if frame.index == frame_index:
                return frame.to_image()

        raise IndexError('no such frame in video')

def _get_frame_sequential2(path, frame_index):
    """sequentially decode until get to frame
    """
    with av.open(path) as container:
        stream = container.streams.video[0]
        #stream.thread_type = "AUTO" # faster but not sure it works
        for packet in container.demux(stream):
            for frame in packet.decode():
                if frame.dts is None:
                    continue 

                if frame.index == frame_index:
                    return frame.to_image()

        raise IndexError('no such frame in video')

import subprocess
import PIL.Image
import os
import tempfile

def _get_frame_ffmpeg_file(path, frame_index):
    import skvideo
    ffmpeg_path = skvideo._FFMPEG_PATH
    cmd = [ f'{ffmpeg_path}/ffmpeg',
            '-y', # overwrite
            "-i",
            path,
            "-vf",
            f"select='eq(n\,{frame_index})'",
            "-vframes",
            "1",
            '-pix_fmt',
            "rgb24",
            "-f",
            "image2",
            '/tmp/frame.png',
        ]
    
    subprocess.check_call(cmd)
    return PIL.Image.open('/tmp/frame.png')

def _get_frame_ffmpeg_pipe(path, frame_index):
    import skvideo
    ffmpeg_path = skvideo._FFMPEG_PATH

    cmd = [ f'{ffmpeg_path}/ffmpeg',
            # '-y', # overwrite
            "-i",
            path,
            "-vf",
            f"select='eq(n\,{frame_index})'",
            "-vframes",
            "1",
            '-pix_fmt',
            "rgb24",
            "-f",
            "image2pipe",
            '-',
        ]

    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    #return PIL.Image.open('/tmp/frame.png')

import copy
from .index import VideoFrameIndex

def _get_packets_v1(path, coords):
    # https://github.com/PyAV-Org/PyAV/pull/287 example using packets
    packets = []
    start_packet = coords['key_packet_idx']
    with av.open(path, 'r') as container:
        stream = container.streams.video[0]
        for packet_no, packet in enumerate(container.demux(stream)):
            if packet_no < start_packet:
                continue
            elif packet_no == start_packet:
                assert packet.is_keyframe
                assert not packet.is_corrupt
            
            new_packet = av.packet.Packet(input=bytes(packet))
            packets.append(new_packet)

            if len(packets) > coords['local_packet_idx']:
                break
        
        codec = av.CodecContext.create(codec=stream.codec_context.name, mode='r')
        codec.extradata = stream.codec_context.extradata

        total_frames = 0
        assert len(packets) > 0
        for pkt in packets:
            for frm in codec.decode(pkt):
                total_frames +=1
        assert total_frames != 0

    return packets, codec

def _make_codec(path):
    container = av.open(path)
    stream = container.streams.video[0]
    extradata = copy.copy(stream.codec_context.extradata)
    codec = av.CodecContext.create(stream.codec_context.name, mode='r')
    codec.extradata = extradata
    return codec

def _decode_up_to(packets, codec_factory, local_packet_no, local_frame_no):
    codec = codec_factory()
    assert local_packet_no < len(packets)
    #print(f'{local_packet_no}:{local_frame_no}')
    for (p,pkt) in enumerate(packets):
        #print(f'{pkt} {pkt.is_keyframe=}')
        for i,frm in enumerate(codec.decode(pkt)):
            #print('currently at', p,i)
            if p == local_packet_no:
                if i == local_frame_no:
                    return frm

    raise IndexError('ran out of packets before getting to frame')

def _get_packets_v2(path, byte_ranges):
    packets = []
    with open(path, 'rb') as fv:
        for (packet_pos, packet_size) in byte_ranges:
            pos = fv.seek(packet_pos)
            assert pos == packet_pos
            packet_bytes = fv.read(packet_size)
            assert len(packet_bytes) == packet_size
            packets.append(av.packet.Packet(input=packet_bytes))

    codec = _make_codec(path)
    return packets, codec

def _get_frame_indexed_internal(path, frame_index, index : VideoFrameIndex, method='seek'):
    """ use available metadata to skip as much work (IO and decode) as possible
    """
    coords = index.get_packet_coordinates(frame_index)

    if method == 'seek':
        byte_ranges = index.get_byte_coords(coords)
        packets, codec = _get_packets_v2(path, byte_ranges)
    elif method == 'packet':
        packets, codec = _get_packets_v1(path, coords)
    else:
        assert False

    frame = _decode_up_to(packets, lambda : codec, coords['local_packet_idx'], coords['local_frame_idx'])
    return frame.to_image()
    
def get_frame(path, frame_index, method='sequential', index : VideoFrameIndex = None) -> PIL.Image.Image:
    mapping = {
        'sequential':_get_frame_sequential2,
        'indexed_packet': lambda a, b : _get_frame_indexed_internal(a, b, index, method='packet'),
        'indexed_seek': lambda a, b : _get_frame_indexed_internal(a, b, index, method='seek'),
    }

    fn = mapping[method]
    return fn(path, frame_index)