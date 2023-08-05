from pathlib import Path
from time import sleep
from typing import Generator
import pytest
import docker
import socket
from docker.models.containers import Container


def find_available_port() -> int:
    with socket.socket() as s:
        s.bind(("", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def postgres_db() -> Generator[str, None, None]:
    """
    Provide a connection string for a PostgreSQL instance
    """
    client = docker.from_env()
    port = find_available_port()
    user = "postgres"
    password = "password"
    db_name = "db"
    container = client.containers.run(
        "postgres:15",
        name=f"data_service_poc_tests_postgres_{port}",
        detach=True,
        ports={5432: port},
        environment={
            "POSTGRES_USER": user,
            "POSTGRES_PASSWORD": password,
            "POSTGRES_DB": db_name,
        },
        volumes={
            Path("tests/fixtures/test_data.sql").absolute(): {
                "bind": "/docker-entrypoint-initdb.d/test_data.sql",
                "mode": "ro",
            }
        },
        remove=True,
    )
    assert isinstance(container, Container)
    sleep(2)  # wait for the container to start
    yield f"postgresql://{user}:{password}@0.0.0.0:{port}/{db_name}"
    container.stop()
    client.close()
