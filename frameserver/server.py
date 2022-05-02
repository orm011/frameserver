from .kfbased import KeyFrameIndex, get_frame2
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from . import readframe as rf
from fastapi.responses import Response
import io
from multiprocessing import Pool
from typing import Optional


pool = Pool(10)

app = FastAPI()
#app.mount("/", StaticFiles(directory="/"), name="static")

@app.get("/{video_path:path}/{keyframe_no}/{frame_no}")
def get_frame(video_path : str, keyframe_no : int, frame_no : int):
    if not video_path.startswith('/'):
        video_path = '/' + video_path

    index = KeyFrameIndex.get(video_path, pool=pool)
    image = get_frame2(video_path, keyframe_no=keyframe_no, frame_no=frame_no, index=index)
    f = io.BytesIO()
    image.save(f, format='PNG')
    return Response(content=f.getvalue(), media_type='image/png')

@app.get("/args/{video_path:path}/")
def get_frame(video_path : str, keyframe_no : int, frame_no : int):
    if not video_path.startswith('/'):
        video_path = '/' + video_path

    index = KeyFrameIndex.get(video_path, pool=pool)
    image = get_frame2(video_path, keyframe_no=keyframe_no, frame_no=frame_no, index=index)
    f = io.BytesIO()
    image.save(f, format='PNG')
    return Response(content=f.getvalue(), media_type='image/png')


if __name__ == '__main__':
    import uvicorn
    ## nb. can also run in command line
    ## uvicorn frameserve.server:app --port 8500 
    uvicorn.run(app, host="127.0.0.1", port=8500, log_level="info")