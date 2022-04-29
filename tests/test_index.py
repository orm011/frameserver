from frameserve.index import VideoFrameIndex, _get_cache_path
import av
import av.datasets
import numpy as np
import pytest
import tempfile

paths = [av.datasets.curated("pexels/time-lapse-video-of-night-sky-857195.mp4"), 
        av.datasets.curated("pexels/time-lapse-video-of-sunset-by-the-sea-854400.mp4")]

import os

os.environ['FRAMESERVER_CACHE'] = tempfile.mkdtemp()

@pytest.mark.parametrize('path', paths)
def test_save(path):
    index0 = VideoFrameIndex._from_video_file(path)
    tempdir = tempfile.mkdtemp()
    index0.save(tempdir)
    index1 = VideoFrameIndex.load(tempdir)
    assert index1.frame_df.shape == index0.frame_df.shape, 'sanity'

@pytest.mark.parametrize('path', paths)
def test_get(path):
    index0 = VideoFrameIndex.get_index(path)

    cpath = _get_cache_path(path)
    index1 = VideoFrameIndex.load(cpath)
    assert index1.frame_df.shape == index0.frame_df.shape, 'sanity'

    ## implicit cache path
    index2 = VideoFrameIndex.get_index(path)
    assert index2.frame_df.shape == index0.frame_df.shape, 'sanity'


