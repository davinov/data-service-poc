from fastapi import FastAPI

from query.query import Query

api = FastAPI()


@api.get("/")
def read_root():
    return {"Hello": "World"}


@api.post("/query/execute")
def execute_query(query: Query):
    return query.execute().to_dicts()  # TODO send arrow format instead
