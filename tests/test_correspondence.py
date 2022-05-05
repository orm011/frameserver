from hashlib import md5
from frameserver.iterator import VideoReader
from frameserver.kfbased import FrameNotFoundException, get_frame, KeyFrameIndex
import pytest
from .shared import *
import numpy as np

frames = [
        dict(path=av.datasets.curated("pexels/time-lapse-video-of-night-sky-857195.mp4"), 
            indices=[   (0,0),(0,1),(0,2),(0,74),(0,75),
                        (1,0),(1,1),(1,2),(1,74),(1,75),
                        (2,0,),(2,1),(2,15)
                    ]
            ),
        dict(path=VIDEO_PATHS['BDD_SAMPLE_MP4'], # keyframes every 15
                indices=[
                        (0,0),(0,1),(0,14),(0,15),(0,16),(1,0),(2,0),(2,1),(3,10),
                        (0,151),(9,0),(9,8) # last frames
                        ],
                beyond_last = [(0,152), (9,9), (10,0)],
                size=(1280, 720)),
        dict(path=VIDEO_PATHS['BIRD_FEEDER_SAMPLE_MP4'], # every 150
                indices=[
                        (0,0),(0,1),(0,149),(0,150),(1,0),(1,1),(1,149),
                        (0,899), (5,0),(5,1),(5,149), # last frames
                        ],
                beyond_last = [(0, 900), (5,150), (6,0)],
                size=(1920,1080))
]

@pytest.mark.parametrize('params', frames)
def test_equal(params):
    """ tests that image from iterator and image from random access are the same
    """
    x = VideoReader(params['path'])
    index = KeyFrameIndex.get(params['path'])

    for tup in x:
        keyframe_no = index.get_keyframe_no(tup['keyframe_pts'])
        key = (keyframe_no, tup['frame_no'])
        if  key in params['indices']:
            print(tup)
            frm2 = get_frame(params['path'], keyframe_no=key[0], frame_no=key[1], index=index)
            assert (np.array(frm2) == np.array(tup['image'])).all()

    bad_keys = [(keyframe_no, key[1] + 1), (keyframe_no+1, 0)]
    for bad_key in bad_keys:
        with pytest.raises(FrameNotFoundException):
            get_frame(params['path'], keyframe_no=bad_key[0], frame_no=bad_key[1], index=index)


@pytest.mark.parametrize('params', frames)
def test_kf_equal(params):
    """ tests that image from iterator and image from random access are the same
            when scanning on keyframe mode
    """
    x = VideoReader(params['path'])
    index = KeyFrameIndex.get(params['path'])

    for (i,tup) in enumerate(x.read(keyframes_only=True)):
        keyframe_no = index.get_keyframe_no(tup['keyframe_pts'])
        key = (keyframe_no, tup['frame_no'])
        assert key[1] == 0
        assert tup['is_keyframe']

        frm2 = get_frame(params['path'], keyframe_no=key[0], frame_no=key[1], index=index)
        assert (np.array(frm2) == np.array(tup['image'])).all()

    ## check read everything
    with pytest.raises(FrameNotFoundException):
        get_frame(params['path'], keyframe_no=i+1, frame_no=0, index=index)

from frameserver.dataset import TorchVideoDataset
from frameserver.util import image_md5
from torch.utils.data import DataLoader
import torch

@pytest.mark.parametrize('params', frames)
def test_getframe_pts(params):
    path = params['path']
    ds = TorchVideoDataset(path, format='image', image_tx=image_md5)
    index = KeyFrameIndex.get(path)

    def process_batches(batches):
        pts = torch.cat([b['pts'] for b in batches])
        pts, indices = torch.sort(pts)
        hashes = np.array(sum([b['image'] for b in batches], []))
        hashes = hashes[indices]
        return pts.numpy(), hashes

    batches = []
    for batch in DataLoader(ds, batch_size=10, num_workers=5):
        batches.append(batch)

    all_pts, all_hashes = process_batches(batches)
    
    sample_idxs = np.random.permutation(np.arange(all_pts.shape[0]))[:20]
    test_pts = all_pts[sample_idxs]
    test_hashes = all_hashes[sample_idxs]

    for _,(pts,hash) in enumerate(zip(test_pts, test_hashes)):
        testframe = get_frame(path, pts=pts, pts_mode='exact', index=index)
        assert image_md5(testframe) == hash