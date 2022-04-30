from frameserve.readframe import get_frame
from frameserve.index import VideoFrameIndex, FFProbe

import av
import av.datasets
import numpy as np
import pytest
import math
import tempfile
import os

testdatadir = f'{os.path.dirname(__file__)}/data/'

frames = [
        dict(path=av.datasets.curated("pexels/time-lapse-video-of-night-sky-857195.mp4"), 
                indices=[0,1,2,72,73,74,75,101,147,148,149,150,164,165], 
                exclude = [73, 74, 148, 149]),
        dict(path=av.datasets.curated("pexels/time-lapse-video-of-sunset-by-the-sea-854400.mp4"), 
                indices=[0,1,2], exclude=[]),
        dict(path=f'{testdatadir}/bdd_b1d0091f-75824d0d_5s.mov', 
                indices=[0,1,2], exclude=[], 
                size=(1280, 720)),
        dict(path=f'{testdatadir}/panama_bird_feeder_sample.tsv',indices=[0,1,2], exclude=[],
                size=(1920,1080))
]

import itertools
os.environ['FRAMESERVER_CACHE'] = tempfile.mkdtemp() # so that indices are re-computed but also shared

@pytest.mark.parametrize('params', frames)
def test_getframe(params):
    """ tests the main function with and without indexing.
    """ 
    path = params['path']
    indices = np.array(params['indices'])
    results = np.zeros_like(indices) - 1
    index = VideoFrameIndex.get_index(path)
    ffprobe = FFProbe.get(path)
        
    spec_size = params.get('size')

    for i,frame_index in enumerate(indices):
        reference = get_frame(path, frame_index, index=None)
        if spec_size:
            assert spec_size == params.get('size')

        try:
            result = get_frame(path, frame_index, index=index)
        except IndexError:
            results[i] = -1
        else:
            results[i] = (np.array(reference) == np.array(result)).all()

    print(np.stack([indices, results]))
    failures = indices[results != 1]

    assert failures.shape[0] == 0, failures

@pytest.mark.parametrize('params,method',  itertools.product(frames, ['indexed_packet']))
def test_getframe_indexed(params, method):
    """ regression: test failures occur only at well known places 
    """
    path = params['path']

    indices = np.array(params['indices'])
    results = np.zeros_like(indices) - 1
    index = VideoFrameIndex.get_index(path)

    reference_method = 'sequential'

    for i,frame_index in enumerate(indices):
        if frame_index in params['exclude']:
            results[i] = -2
            continue

        reference = get_frame(path, frame_index, method=reference_method)

        try:
            result = get_frame(path, frame_index, method=method, index=index, fallback=False)
        except IndexError:
            results[i] = -1
        else:
            results[i] = (np.array(reference) == np.array(result)).all()

    print(np.stack([indices, results]))
    failures = indices[(results != 1) & (results != -2)]

    assert failures.shape[0] == 0, failures