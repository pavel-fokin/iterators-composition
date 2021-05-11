# pylint:disable=attribute-defined-outside-init
import json
import os.path
from functools import reduce

import requests


def compose(*functions):
    def _compose(f, g):  # pylint:disable=invalid-name
        return lambda *args: g(f(*args))

    return reduce(_compose, functions)


class Load:
    """Load data from a url"""

    def __init__(
        self, *, url: str, api_key: str, start_date: str, end_date: str = ""
    ):
        if end_date:
            self.url = url.format(start_date, end_date, api_key)
        self.url = url.format(start_date, api_key)

    @staticmethod
    def test_data():
        with open("test_data.json") as file:
            data = json.loads(file.read())
        return data["near_earth_objects"]

    def __call__(self):
        payload = requests.get(self.url)
        data = payload.json()
        return data["near_earth_objects"]


class Write:
    """Write data with asteroids to files by date"""

    def __init__(self, *, to_path: str):
        self.to_path = to_path

    def __call__(self, iterator):
        self.iterator = iterator
        return self

    def _process(self, date, objects):
        filepath = os.path.join(self.to_path, f"{date}.json")
        with open(filepath, "w") as file:
            file.write(json.dumps(objects))

    def __iter__(self):
        for date, objects in self.iterator.items():
            self._process(date, objects)
            yield date, objects


class Find:
    """Find hazardous asteroid"""

    def __init__(self, *, field: str):
        self.field = field

    def __call__(self, iterator):
        self.iterator = iterator
        return self

    def __iter__(self):
        for _date, objects in self.iterator:
            for item in objects:
                if item.get(self.field):
                    yield item
