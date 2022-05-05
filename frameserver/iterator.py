from frameserver.kfbased import KeyFrameIndex
import av
from .util import get_image_rotation_tx

## implements iterable interface
class VideoReader:
    def __init__(self, video_path, index : KeyFrameIndex = None):
        self.video_path = video_path
        self.rotation_fix_tx = get_image_rotation_tx(video_path)
        self.index = index

    def _frame_tx(self, frame):
        im = frame.to_image()
        im = self.rotation_fix_tx(im)
        return im 
        
    def _iter_internal(self, container, stream, *, keyframes_only : bool, start_pts : int, end_pts : int):
        if keyframes_only:
            stream.codec_context.skip_frame = "NONKEY"

        if start_pts is not None:
            if self.index:
                kfno = self.index.get_keyframe_no(start_pts)
                seek_pts = self.index.get_pts(kfno)
            else:
                seek_pts = start_pts

            container.seek(offset=seek_pts, backward=True, any_frame=False, stream=stream)

        for i,frame in enumerate(container.decode(stream)):
            if i == 0:
                if start_pts is not None and start_pts >= stream.start_time:
                    assert frame.pts <= start_pts, f'seek did not work as expected. this may be due to the format. either use an index or switch container format'
                    # some formats like .mpeg ts seem to jump to keyframes ahead of the timestamp
                    # this creates problems in partitioning bc we cannot reliably access the frames we are assigned.

            if frame.key_frame:
                keyframe_pts = int(frame.pts)
                current_frame_no = 0
            
            tup =  {'keyframe_pts': keyframe_pts,  # identifies this frame's predecessor keyframe
                    'frame_no':current_frame_no, 
                    'pts':int(frame.pts), 
                    'time_s':frame.time,
                    'is_keyframe':frame.key_frame,
                    'image':frame
                    }
            current_frame_no += 1
            
            if start_pts is not None and frame.pts < start_pts: # don't include
                    continue

            if end_pts is not None and frame.pts >= end_pts: # finished
                    break
            yield tup

    def read(self, *, metadata_only=False, keyframes_only=False, start_pts : int = None, end_pts : int = None):
        if end_pts is not None and start_pts is not None:
            if not end_pts > start_pts:
                assert False
            
        with av.open(self.video_path, 'r') as container:
            stream = container.streams.video[0]
            for tup in self._iter_internal(container, stream, keyframes_only=keyframes_only, start_pts=start_pts, end_pts=end_pts):
                frame = tup['image']
                del tup['image']

                if not metadata_only:
                    tup['image'] = self._frame_tx(frame)
                
                yield tup

    def __iter__(self):
        return self.read()