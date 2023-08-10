def test_simple_query(client):
    response = client.post(
        "/query/execute",
        json={
            "source": {
                "type": "file",
                "file": "tests/fixtures/sample_data.csv",
            },
            "steps": [],
        },
    )
    assert response.status_code == 200
    assert response.json() == [
        {"user_id": "A", "value": 1},
        {"user_id": "B", "value": 2},
        {"user_id": "C", "value": 3},
    ]
