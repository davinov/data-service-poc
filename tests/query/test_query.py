from pathlib import Path

from query.query import Query
from query.sources import DatabaseSource, FileSource
from query.steps import FilterStep, JoinStep, RandomStep


def test_query_from_file():
    table = Query(
        source=FileSource(file=Path("tests/fixtures/sample_data.csv")), steps=[]
    ).execute()
    assert table.shape == (3, 2)


def test_query_from_db(postgres_db: str):
    table = Query(
        source=DatabaseSource(connection_uri=postgres_db, table="sample_data"), steps=[]
    ).execute()
    assert table.shape == (3, 2)


def test_query_from_file_with_filtering():
    table = Query(
        source=FileSource(file=Path("tests/fixtures/sample_data.csv")),
        steps=[FilterStep(column="user_id", value="B")],
    ).execute()
    assert table.shape == (1, 2)


def test_query_from_db_with_filtering(postgres_db):
    table = Query(
        source=DatabaseSource(connection_uri=postgres_db, table="sample_data"),
        steps=[FilterStep(column="user_id", value="B")],
    ).execute()
    assert table.shape == (1, 2)


def test_query_from_file_with_random():
    table = Query(
        source=FileSource(file=Path("tests/fixtures/sample_data.csv")),
        steps=[RandomStep(new_column="random")],
    ).execute()
    assert table.shape == (3, 3)


def test_query_from_db_with_random(postgres_db):
    table = Query(
        source=DatabaseSource(connection_uri=postgres_db, table="sample_data"),
        steps=[RandomStep(new_column="random")],
    ).execute()
    assert table.shape == (3, 3)


def test_join_query_from_db_with_other_query_from_same_db(postgres_db):
    table = Query(
        source=DatabaseSource(connection_uri=postgres_db, table="sample_data"),
        steps=[
            JoinStep(
                right_query=Query(
                    source=DatabaseSource(connection_uri=postgres_db, table="users"),
                    steps=[],
                ),
                on="user_id",
            )
        ],
    ).execute()
    assert table.shape == (3, 3)


def test_join_query_from_db_with_other_query_different_db(postgres_db):
    # Simulate that we connect to two different DBs
    alternative_postgres_db = postgres_db.replace("0.0.0.0", "localhost")

    table = Query(
        source=DatabaseSource(connection_uri=postgres_db, table="sample_data"),
        steps=[
            JoinStep(
                right_query=Query(
                    source=DatabaseSource(
                        connection_uri=alternative_postgres_db, table="users"
                    ),
                    steps=[],
                ),
                on="user_id",
            )
        ],
    ).execute()
    assert table.shape == (3, 3)


def test_join_query_from_db_with_file(postgres_db):
    table = Query(
        source=DatabaseSource(connection_uri=postgres_db, table="sample_data"),
        steps=[
            JoinStep(
                right_query=Query(
                    source=FileSource(file=Path("tests/fixtures/users.csv")),
                    steps=[],
                ),
                on="user_id",
            ),
        ],
    ).execute()
    assert table.shape == (3, 3)
