import random
import uuid

import pytest
from faker import Faker

from odetam.query import DetaQuery

TEST_UUID = uuid.uuid4()

print("unique test id", TEST_UUID)


@pytest.fixture
def unique_test_id():
    return str(TEST_UUID)


@pytest.fixture
def query_one():
    return DetaQuery(condition="condition1", value="value1")


@pytest.fixture
def query_two():
    return DetaQuery(condition="condition2", value="value2")


@pytest.fixture
def query_three():
    return DetaQuery(condition="condition3", value="value3")


@pytest.fixture
def query_four():
    return DetaQuery(condition="condition4", value="value4")


@pytest.fixture
def random_captain_data():
    fake = Faker()

    def _gen_captain_data(with_key=False):
        item = {
            "name": fake.name(),
            "joined": int(fake.future_date().strftime("%Y%m%d")),
            "ships": [fake.company() for _ in range(0, random.randint(1, 10))],
        }
        if with_key:
            item["key"] = f"key{random.randint(0, 100)}"
        return item

    return _gen_captain_data


@pytest.fixture
def make_bunch_of_random_captains(random_captain_data):
    def _generate(captain_class, count: int = 52):
        captains = []
        captain_data = []
        captain_data_with_keys = []
        # generate random data and store as Captain object, dict, and dict with key
        for _ in range(count):
            captain = random_captain_data()
            captain_data.append(captain)
            captains.append(captain_class._deserialize(captain))
            captain_data_with_keys.append(
                {**captain, "key": f"key{random.randint(0, 500)}"}
            )
        return captains, captain_data, captain_data_with_keys

    return _generate
