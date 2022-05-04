from .kfbased import *
    
## implements iterable interface so it can be used eg as a torch dataset
class VideoIterator:
    def __init__(self, video_path, keyframes_only=False, meta_only=False, image_tx=None):
        self.video_path = video_path
        self.keyframes_only = keyframes_only
        self.meta_only = meta_only
        self.image_tx = image_tx

    def _full_iter(self, container, stream):
        current_keyframe = -1
        current_frame_no = 0
        if self.keyframes_only:
            stream.codec_context.skip_frame = "NONKEY"

        for frame in enumerate(container.decode(stream)):
            if frame.key_frame:
                current_keyframe +=1
                current_frame_no = 0
            
            yield {'keyframe_no':current_keyframe, 
                    'frame_no':current_frame_no, 'pts':frame.pts, 
                    'time':frame.time,
                    'is_keyframe':frame.key_frame,
                    'image':frame 
                }
            current_frame_no += 1

    def __iter__(self):
        with av.open(self.video_path, 'r') as container:
            stream = container.streams.video[0]
            iterator = self._full_iter(container, stream)

            for tup in iterator:
                if self.meta_only:
                    del tup['image']
                    yield tup
                
                tup['image'] = tup['image'].to_image()
                if self.image_tx:
                    tup['image'] = self.image_tx(tup['image'])
                yield tup