import av
import av.datasets
import frameserver
import itertools
import os
import sys
import tempfile

os.environ['FRAMESERVER_CACHE'] = tempfile.mkdtemp() # so that indices are re-computed but also shared
TESTDATADIR = f'{os.path.dirname(__file__)}/data/'

VIDEO_PATHS = {
    'NIGHT_SKY':av.datasets.curated("pexels/time-lapse-video-of-night-sky-857195.mp4"),
    'BDD_SAMPLE':f'{TESTDATADIR}/bdd_b1d0091f-75824d0d_5s.mov',
    'BIRD_FEEDER_SAMPLE':f'{TESTDATADIR}/panama_bird_feeder_sample.tsv',
}
