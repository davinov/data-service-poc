from .client import client

def test_root(client):
    response = client.get('/')
    assert response.status_code == 200
    assert response.json() == {"Hello": "World"}


