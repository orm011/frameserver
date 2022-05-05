import torch
import torchvision.transforms as T
from .iterator import VideoReader
from .util import get_pts_intervals
import av

class TorchVideoDataset(torch.utils.data.IterableDataset):
    def __init__(self, path, keyframes_only=False, image_tx=None):
        txs = [T.ToTensor()]
        if image_tx is not None:
            txs.append(image_tx)
        self.tx = T.Compose(txs)
        self.video_reader = VideoReader(path)
        self.keyframes_only = keyframes_only
        
    def __iter__(self):
        worker_info = torch.utils.data.get_worker_info()
        if worker_info is None:
            # single-process data loading, return the full iterator            
            for tup in self.video_reader.read(keyframes_only=self.keyframes_only):
                tup['image'] = self.tx(tup['image'])
                yield tup
        else:
            with av.open(self.video_reader.video_path, 'r') as container:
                stream = container.streams.video[0]
                intervals = get_pts_intervals(stream, num_tasks=worker_info.num_workers)
                start_pts, end_pts = intervals[worker_info.id]
                for tup in self.video_reader.read(keyframes_only=self.keyframes_only, start_pts=start_pts, end_pts=end_pts):
                    tup['image'] = self.tx(tup['image'])
                    yield tup



