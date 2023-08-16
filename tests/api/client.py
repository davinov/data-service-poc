from fastapi.testclient import TestClient
import pytest

from api.main import api


@pytest.fixture(scope="module")
def client():
    with TestClient(api) as client:
        yield client
