from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from . import readframe as rf
from fastapi.responses import Response
import io

from typing import Optional

app = FastAPI()
#app.mount("/", StaticFiles(directory="/"), name="static")

@app.get("/getframe")
def get_frame(fpath : str, frame : Optional[int] = None):
    print(fpath, frame)
    image = rf.get_frame(fpath, frame)
    f = io.BytesIO()
    image.save(f, format='PNG')
    f.seek(0)
    return Response(content=f.read(), media_type='image/png')