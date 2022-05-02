from tests.shared import VIDEO_PATHS
from fastapi.testclient import TestClient
from frameserver.server import app
import PIL.Image
import io

client = TestClient(app)

def test_read_main():
    url = f"{VIDEO_PATHS['BDD_SAMPLE']}/0/0"
    print(url)
    response = client.get(url)
    assert response.status_code == 200
    assert response.headers.get('content-type') == 'image/png'
    f = io.BytesIO(response.content)
    img = PIL.Image.open(f)
    img.load()


def test_args():
    url = f"/args/{VIDEO_PATHS['BDD_SAMPLE']}?keyframe_no={1}&frame_no={1}"
    print(url)
    response = client.get(url)
    print(response)
    assert response.status_code == 200
    assert response.headers.get('content-type') == 'image/png'
    f = io.BytesIO(response.content)
    img = PIL.Image.open(f)
    img.load()