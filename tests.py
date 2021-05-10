import json
import os.path
from unittest.mock import patch

import pipelines


def data_fixture():
    with open("test_data.json") as file:
        data = json.loads(file.read())
    return data["near_earth_objects"]


@patch("pipelines.Load.__call__", return_value=[1, 2, 3])
def test_load(_load=None):
    load = pipelines.Load(url="url", start_date="today", api_key="secret_key")
    result = load()
    assert result == [1, 2, 3]


def test_write():
    write = pipelines.Write(to_path="/tmp")
    result = write({"today": "data"})

    assert os.path.exists("/tmp/today.json")
    assert list(result) == [("today", "data")]


def test_find():
    find = pipelines.Find(field="find_me")
    result = find([("today", [{"find_me": True}, {"not_find_me": True}])])

    assert list(result) == [{"find_me": True}]


@patch("pipelines.Load.__call__", return_value=data_fixture())
def test_integration(_load_mock=None):
    load = pipelines.Load(url="url", start_date="today", api_key="secret_key")
    write = pipelines.Write(to_path="/tmp")
    find = pipelines.Find(field="is_potentially_hazardous_asteroid")

    pipeline = pipelines.compose(load, write, find)

    result = list(pipeline())
    print(result)

    assert len(result) == 1


if __name__ == "__main__":
    test_load()
    test_write()
    test_find()
    test_integration()
