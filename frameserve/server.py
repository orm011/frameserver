from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from . import readframe as rf
from fastapi.responses import Response
import io

from typing import Optional

app = FastAPI()
#app.mount("/", StaticFiles(directory="/"), name="static")

@app.get("/{video_path:path}/{frame_index}")
def get_frame(video_path : str, frame_index : int):
    if not video_path.startswith('/'):
        video_path = '/' + video_path

    image = rf.get_frame(video_path, frame_index, method='indexed_seek')
    f = io.BytesIO()
    image.save(f, format='PNG')
    return Response(content=f.getvalue(), media_type='image/png')