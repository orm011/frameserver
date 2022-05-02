from tests.shared import VIDEO_PATHS
from fastapi.testclient import TestClient
from frameserver.server import app
import PIL.Image
import io

client = TestClient(app)

def test_frame():
    url = f"/frame/{VIDEO_PATHS['BDD_SAMPLE']}?keyframe_no={1}&frame_no={1}"
    response = client.get(url)
    print(response)
    assert response.status_code == 200
    assert response.headers.get('content-type') == 'image/png'
    f = io.BytesIO(response.content)
    img = PIL.Image.open(f)
    img.load()