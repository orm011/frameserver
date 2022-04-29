from frameserve.readframe import get_frame
from frameserve.index import VideoFrameIndex

import av
import av.datasets
import numpy as np
import pytest

inputs = [
        dict(path=av.datasets.curated("pexels/time-lapse-video-of-night-sky-857195.mp4"), 
                indices=[0,1,2,72, 73, 149, 150, 164, 165]),
        dict(path=av.datasets.curated("pexels/time-lapse-video-of-sunset-by-the-sea-854400.mp4"), 
                indices=[0,1,2]),
]

@pytest.mark.parametrize('params', inputs)
def test_getframe_indexed(params):
    path = params['path']
    indices = np.array(params['indices'])
    results = np.zeros_like(indices) - 1
    index = VideoFrameIndex.from_video_file(path)

    reference_method = 'sequential'

    for i,frame_index in enumerate(indices):
        reference = get_frame(path, frame_index, method=reference_method, index=index)

        try:
            result = get_frame(path, frame_index, method='indexed_seek', index=index)
        except IndexError:
            results[i] = -1
        else:    
            results[i] = (np.array(reference) == np.array(result)).all()

    print(np.stack([indices, results]))
    failures = indices[results != 1]

    assert failures.shape[0] == 0, failures