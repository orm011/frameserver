from frameserve.readframe import get_frame
from frameserve.index import VideoFrameIndex

import av
import av.datasets
import numpy as np
import pytest
import math


inputs = [
        dict(path=av.datasets.curated("pexels/time-lapse-video-of-night-sky-857195.mp4"), 
                indices=[0,1,2,72,73,74,75,101,147,148,149,150,164,165], 
                exclude = [73, 74, 148, 149]),
        dict(path=av.datasets.curated("pexels/time-lapse-video-of-sunset-by-the-sea-854400.mp4"), 
                indices=[0,1,2], exclude=[]),
]   

@pytest.mark.parametrize('params', inputs)
def test_getframe(params):
    path = params['path']
    indices = np.array(params['indices'])
    results = np.zeros_like(indices) - 1
    index = VideoFrameIndex._from_video_file(path)

    reference_method = 'sequential'

    for i,frame_index in enumerate(indices):
        reference = get_frame(path, frame_index, method=reference_method, index=index)

        try:
            result = get_frame(path, frame_index, method='indexed_seek', index=index, fallback=True)
        except IndexError:
            results[i] = -1
        else:
            results[i] = (np.array(reference) == np.array(result)).all()

    print(np.stack([indices, results]))
    failures = indices[results != 1]

    assert failures.shape[0] == 0, failures

@pytest.mark.parametrize('params', inputs)
def test_getframe_indexed(params):
    """ regression: test failures occur only at well known places 
    """
    path = params['path']

    indices = np.array(params['indices'])
    results = np.zeros_like(indices) - 1
    index = VideoFrameIndex._from_video_file(path)

    reference_method = 'sequential'

    for i,frame_index in enumerate(indices):
        if frame_index in params['exclude']:
            results[i] = -2
            continue

        reference = get_frame(path, frame_index, method=reference_method, index=index)

        try:
            result = get_frame(path, frame_index, method='indexed_seek', index=index, fallback=False)
        except IndexError:
            results[i] = -1
        else:
            results[i] = (np.array(reference) == np.array(result)).all()

    print(np.stack([indices, results]))
    failures = indices[(results != 1) & (results != -2)]

    assert failures.shape[0] == 0, failures