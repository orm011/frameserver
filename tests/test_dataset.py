import torch
from torch.utils.data import DataLoader
import pytest
from frameserver.dataset import TorchVideoDataset
from .shared import *
from frameserver.util import image_md5
videos = [
        av.datasets.curated("pexels/time-lapse-video-of-night-sky-857195.mp4"),
        VIDEO_PATHS['BDD_SAMPLE_MP4'],
        VIDEO_PATHS['BIRD_FEEDER_SAMPLE_MP4'], 
]

from itertools import product
import numpy as np

@pytest.mark.parametrize('path,num_workers', list(product(videos, [11])))
def test_dataloader_all(path, num_workers):
    vd = TorchVideoDataset(path, image_tx=image_md5)

    def process_batches(batches):
        pts = torch.cat([b['pts'] for b in batches])
        pts, indices = torch.sort(pts)
        hashes = np.array(sum([b['image'] for b in batches], []))
        hashes = hashes[indices]
        return pts.numpy(), hashes

    batches_serial = []
    for batch in DataLoader(vd, batch_size=13):
        batches_serial.append(batch)

    batches_parallel = []
    for batch in DataLoader(vd, num_workers=num_workers, batch_size=13):
        batches_parallel.append(batch)

    pts0,hashes0 = process_batches(batches_serial)
    pts1,hashes1 = process_batches(batches_parallel)
    assert pts0.shape == pts1.shape
    assert (pts0 == pts1).all()
    assert (hashes0 == hashes1).all()
