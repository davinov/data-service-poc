Proof of concept for a query execution service which could:
- [-] execute queries from diverse sources
  - [x] from local files
  - [x] from SQL databases
  - [ ] from cloud storages (S3)
  - [ ] from online services (HTTP APIs)
- [x] combine queries from multiple sources
- [x] push as much work as possible upstream (to the source DB) and execute the rest in-memory
- [ ] track time and resources used by query
- [ ] handle many concurrent clients
- [ ] limit memory usage per client
- [x] save queries to a transient cache
- [ ] execute data processing jobs and stream the result to disk

## Setup

`poetry install`

## Tests

`poetry run pytest`

## Run

`poetry run uvicorn api.main:api`
