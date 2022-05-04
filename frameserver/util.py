import skvideo
import PIL.Image

_rotation_map = {'90':PIL.Image.Transpose.ROTATE_90,
                '270':PIL.Image.Transpose.ROTATE_270,
                '180':PIL.Image.Transpose.ROTATE_180,
                '0':None,
                }

class FFProbe:
    def __init__(self, ffprobe):
        self.ffprobe = ffprobe if ffprobe else {}

    @staticmethod
    def get(path):
        ffprobe = skvideo.io.ffprobe(path)
        return FFProbe(ffprobe)

    def get_rotation(self):
        rot =  (self.ffprobe.get('video', {})
            .get('side_data_list', {})
            .get('side_data', {})
            .get('@rotation', None))

        return _rotation_map.get(rot, None)

    def get_frame_size(self):
        x = self.ffprobe
        width = int(x.get('video',{}).get('@width', None))
        height = int(x.get('video',{}).get('@height', None))

        rot = self.get_rotation()
        if rot in [PIL.Image.Transpose.ROTATE_90, PIL.Image.Transpose.ROTATE_270]:
            tmp = width
            width = height
            height = tmp
        
        return (width, height)


class FixRotationTx:
    def __init__(self, rotation):
        self.rotation = rotation

    def __call__(self, image):
        if self.rotation:
            return image.transpose(method=self.rotation)
        else:
            return image

def get_image_rotation_tx(path):
    ffprobe = FFProbe.get(path)
    rotation = ffprobe.get_rotation()
    return FixRotationTx(rotation)