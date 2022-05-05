from .kfbased import KeyFrameIndex, get_frame, FrameNotFoundException
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import Response, HTMLResponse
import io
from multiprocessing import Pool
from typing import Optional
import os

pool = Pool(10)

app = FastAPI()
app.mount("/static/", StaticFiles(directory="/"), name="static")


@app.get("/frame/{video_path:path}")
def get_frame_args(video_path : str, keyframe_no : int = None, frame_no : int = 0, pts : int = None):
    if not video_path.startswith('/'):
        video_path = '/' + video_path

    if not os.path.isfile(video_path):
        raise HTTPException(status_code=404, detail=f'video {video_path} does not exist')

    if pts is None: 
        index = KeyFrameIndex.get(video_path, pool=pool)
    else: # no need for index if we're using pts directly
        index = None

    try:
        image = get_frame(video_path, keyframe_no=keyframe_no, frame_no=frame_no, pts=pts, pts_mode='exact', index=index)
    except FrameNotFoundException:
        raise HTTPException(status_code=404, detail=f'frame {keyframe_no=} {frame_no=} does not exist in video')

    f = io.BytesIO()
    image.save(f, format='PNG')
    return Response(content=f.getvalue(), media_type='image/png')

@app.get("/video_meta/{video_path:path}")
def get_meta(video_path : str):
    if not video_path.startswith('/'):
        video_path = '/' + video_path

    if not os.path.isfile(video_path):
        raise HTTPException(status_code=404, detail=f'video {video_path} does not exist')

    index = KeyFrameIndex.get(video_path, pool=pool)

    return HTMLResponse(content=index.df.to_html())

if __name__ == '__main__':
    import uvicorn
    ## nb. can also run in command line
    ## uvicorn frameserve.server:app --port 8500 
    uvicorn.run(app, host="127.0.0.1", port=8500, log_level="info")