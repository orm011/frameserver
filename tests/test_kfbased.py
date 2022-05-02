import tempfile
from frameserver.kfbased import KeyFrameIndex, get_frame, _get_keyframe_index, _get_frame_reference_impl
import pytest
import av
import os
import multiprocessing

from .shared import *

frames = [
        dict(path=av.datasets.curated("pexels/time-lapse-video-of-night-sky-857195.mp4"), 
            indices=[   (0,0),(0,1),(0,2),(0,74),(0,75),
                        (1,0),(1,1),(1,2),(1,74),(1,75),
                        (2,0,),(2,1),(2,15)
                    ]
            ),
        dict(path=VIDEO_PATHS['BDD_SAMPLE'], # keyframes every 15
                indices=[
                        (0,0),(0,1),(0,14),(0,15),(0,16),(1,0),(2,0),(2,1),(3,10),
                        (0,151),(9,0),(9,8) # last frames
                        ],
                beyond_last = [(0,152), (9,9), (10,0)],
                size=(1280, 720)),
        dict(path=VIDEO_PATHS['BIRD_FEEDER_SAMPLE'], # every 150
                indices=[
                        (0,0),(0,1),(0,149),(0,150),(1,0),(1,1),(1,149),
                        (0,899), (5,0),(5,1),(5,149), # last frames
                        ],
                beyond_last = [(0, 900), (5,150), (6,0)],
                size=(1920,1080))
]

@pytest.fixture(scope='module')
def pool():
    pool = multiprocessing.Pool(10)
    return pool


@pytest.mark.parametrize('params', frames)
def test_parallel(params, pool):
    path = params['path']
    pdf = _get_keyframe_index(path, pool, granularity='8 seconds')
    df = _get_keyframe_index(path)
    assert pdf.shape == df.shape


@pytest.mark.parametrize('params', frames)
def test_kfindex(params, pool):
    path = params['path']
    index = KeyFrameIndex._from_video_file(path, pool)
    pts = index.get_pts(keyframe_no=0)

    with av.open(path, 'r') as container:
        s = container.streams.video[0]
        start_time = int(s.start_time)

    assert start_time  == pts

@pytest.mark.parametrize('params', frames)
def test_store_load_kfindex(params, pool):
    path = params['path']
    index = KeyFrameIndex._from_video_file(path, pool)
    pts = index.get_pts(keyframe_no=1)
    temp = tempfile.mkdtemp()
    index.save(temp)
    index2 = KeyFrameIndex.load(temp)
    pts2 = index2.get_pts(keyframe_no=1)
    assert pts == pts2


@pytest.mark.parametrize('params', frames)
def test_getframe(params, pool):
    path = params['path']
    index = KeyFrameIndex.get(path, pool)
    for (keyframe_no, frame_no) in params['indices']:
        print(keyframe_no, frame_no)
        reference = _get_frame_reference_impl(path, keyframe_no=keyframe_no, frame_no=frame_no)
        testframe1 = get_frame(path, keyframe_no=keyframe_no, frame_no=frame_no)
        testframe_index = get_frame(path, keyframe_no=keyframe_no, frame_no=frame_no, index=index)

        assert testframe1 == reference
        assert testframe_index == reference