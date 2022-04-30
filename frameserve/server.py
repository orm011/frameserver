from frameserve.index import VideoFrameIndex
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

    index = VideoFrameIndex.get_index(video_path)
    image = rf.get_frame(video_path, frame_index, index=index)
    f = io.BytesIO()
    image.save(f, format='PNG')
    return Response(content=f.getvalue(), media_type='image/png')


if __name__ == '__main__':
    import uvicorn
    ## nb. can also run in command line
    ## uvicorn frameserve.server:app --port 8500 
    uvicorn.run(app, host="127.0.0.1", port=8500, log_level="info")