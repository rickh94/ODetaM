import datetime
import os
import random
from typing import List
from unittest import mock

import pytest
from faker import Faker

from odetam import __version__, DetaModel
from odetam.exceptions import NoProjectKey, ItemNotFound, DetaError
from odetam.field import DetaField


@pytest.fixture
def Captain():
    class _Captain(DetaModel):
        name: str
        joined: datetime.date
        ships: List[str]

    return _Captain


@pytest.fixture
def captains(Captain):
    return [
        Captain(
            name="James T. Kirk",
            joined=datetime.date(2252, 1, 1),
            ships=["Enterprise", "Enterprise-A"],
        ),
        Captain(
            name="Benjamin Sisko",
            joined=datetime.date(2350, 1, 1),
            ships=["Deep Space 9", "Defiant"],
        ),
    ]


@pytest.fixture
def captains_list():
    return [
        {
            "name": "James T. Kirk",
            "joined": "2252-01-01",
            "ships": ["Enterprise", "Enterprise-A"],
        },
        {
            "name": "Benjamin Sisko",
            "joined": "2350-01-01",
            "ships": ["Deep Space 9", "Defiant"],
        },
    ]


@pytest.fixture
def captains_with_keys_list(captains_list):
    return [
        {
            "name": "James T. Kirk",
            "joined": "2252-01-01",
            "ships": ["Enterprise", "Enterprise-A"],
            "key": "key1",
        },
        {
            "name": "Benjamin Sisko",
            "joined": "2350-01-01",
            "ships": ["Deep Space 9", "Defiant"],
            "key": "key2",
        },
    ]


@pytest.fixture
def random_captain_data():
    fake = Faker()

    def _gen_captain_data(with_key=False):
        item = {
            "name": fake.name(),
            "joined": str(fake.future_date()),
            "ships": [fake.company() for _ in range(0, random.randint(1, 10))],
        }
        if with_key:
            item["key"] = f"key{random.randint(0, 100)}"
        return item

    return _gen_captain_data


def test_version():
    assert __version__ == "0.1.0"


def test_deta_meta_model_class_creates_db(monkeypatch):
    deta_mock = mock.MagicMock()
    instance_mock = mock.MagicMock()
    deta_mock.return_value = instance_mock
    db = mock.MagicMock()
    instance_mock.Base.return_value = db
    monkeypatch.setattr("odetam.model.Deta", deta_mock)

    class ObjectExample(DetaModel):
        name: str

    deta_mock.assert_called_with("123_123")
    instance_mock.Base.assert_called_with("object_example")
    assert ObjectExample.__db_name__ == "object_example"
    assert ObjectExample.__db__ == db


def test_deta_meta_model_raises_error_with_no_project_key():
    os.environ["PROJECT_KEY"] = ""
    with pytest.raises(NoProjectKey):

        class ObjectExample(DetaModel):
            pass

    os.environ["PROJECT_KEY"] = "123_123"


def test_deta_meta_model_assigns_fields():
    class ObjectExample(DetaModel):
        name: str
        age: int

    assert isinstance(ObjectExample.name, DetaField)
    assert isinstance(ObjectExample.age, DetaField)


def test_get(Captain):
    db_mock = mock.MagicMock()
    Captain.__db__ = db_mock
    db_mock.get.return_value = {
        "name": "Jean-Luc Picard",
        "joined": "2323-01-01",
        "ships": ["Enterprise-D", "Enterprise-E", "La Sirena"],
        "key": "key5",
    }
    picard = Captain.get("key5")

    db_mock.get.assert_called_with("key5")

    assert picard.name == "Jean-Luc Picard"
    assert picard.joined == datetime.date(2323, 1, 1)
    assert picard.ships == ["Enterprise-D", "Enterprise-E", "La Sirena"]
    assert picard.key == "key5"


def test_get_not_found_raises_error(Captain):
    db_mock = mock.MagicMock()
    Captain.__db__ = db_mock
    db_mock.get.return_value = None
    with pytest.raises(ItemNotFound):
        Captain.get("key1")


def test_get_all(Captain, captains_with_keys_list):
    db_mock = mock.MagicMock()
    Captain.__db__ = db_mock

    def _mock_fetch():
        yield captains_with_keys_list

    db_mock.fetch = _mock_fetch

    records = Captain.get_all()
    assert len(records) == len(captains_with_keys_list)
    for record in records:
        assert isinstance(record, Captain)
    for captain in captains_with_keys_list:
        assert Captain.parse_obj(captain) in records


def test_query(Captain, captains_with_keys_list):
    def _mock_fetch(query_statement):
        assert query_statement["example"] == "query"

        yield [captains_with_keys_list[1]]

    Captain.__db__.fetch = _mock_fetch
    mock_query_statement = mock.MagicMock()
    mock_query_statement.as_query.return_value = {"example": "query"}
    results = Captain.query(mock_query_statement)

    assert len(results) == 1
    assert Captain.parse_obj(captains_with_keys_list[1]) in results


def test_delete_key(Captain):
    Captain.__db__ = mock.MagicMock()

    Captain.delete_key("testkey")
    Captain.__db__.delete.assert_called_with("testkey")


def test_put_many(Captain, captains_list, captains_with_keys_list, captains):
    db_mock = mock.MagicMock()
    db_mock.put_many.return_value = {"processed": {"items": captains_with_keys_list}}
    Captain.__db__ = db_mock
    results = Captain.put_many(captains)

    db_mock.put_many.assert_called_with(captains_list)

    assert len(results) == len(captains)
    for captain in captains_with_keys_list:
        assert Captain.parse_obj(captain) in results


def test_put_more_than_25(Captain, random_captain_data):
    """Test put many is correctly batched for lists of items larger than
    25.
    """
    captains = []
    captain_data = []
    captain_data_with_keys = []
    # generate random data and store as Captain object, dict, and dict with key
    for _ in range(52):
        captain = random_captain_data()
        captain_data.append(captain)
        captains.append(Captain.parse_obj(captain))
        captain_data_with_keys.append(
            {**captain, "key": f"key{random.randint(0, 500)}"}
        )
    db_mock = mock.MagicMock()
    # the three calls should have batches of 25 and the api will return process data
    # in this form
    db_mock.put_many.side_effect = [
        {"processed": {"items": captain_data_with_keys[0:25]}},
        {"processed": {"items": captain_data_with_keys[25:50]}},
        {"processed": {"items": captain_data_with_keys[50:]}},
    ]
    Captain.__db__ = db_mock
    results = Captain.put_many(captains)

    # Two batches of 25, and the remaining records
    assert db_mock.put_many.call_count == 3
    # put_many should have been called with each of the batches
    db_mock.put_many.assert_any_call(captain_data[0:25])
    db_mock.put_many.assert_any_call(captain_data[25:50])
    db_mock.put_many.assert_any_call(captain_data[50:])

    # resulting data should have been flattened and serialized, and have keys
    # added from database result
    for item in captain_data_with_keys:
        assert Captain.parse_obj(item) in results


def test_save_new(Captain):
    db_mock = mock.MagicMock()
    Captain.__db__ = db_mock
    data = {"name": "Saru", "joined": "2249-01-01", "ships": ["Discovery"]}
    db_mock.put.return_value = {**data, "key": "key8"}
    saru = Captain.parse_obj(data)
    saru.save()

    db_mock.put.assert_called_with(data)
    assert saru.key == "key8"


def test_save_existing(Captain):
    data = {
        "name": "Katheryn Janeway",
        "joined": "2360-01-01",
        "ships": ["Voyager"],
        "key": "key25",
    }
    db_mock = mock.MagicMock()
    db_mock.put.return_value = data
    Captain.__db__ = db_mock
    janeway = Captain.parse_obj(data)
    janeway.save()
    db_mock.put.assert_called_with(data)
    assert janeway.key == "key25"


def test_delete(captains, Captain):
    db_mock = mock.MagicMock()
    captains[0].key = "key22"
    Captain.__db__ = db_mock
    captains[0].delete()
    db_mock.delete.assert_called_with("key22")
    assert captains[0].key is None


def test_delete_no_key(Captain, captains):
    db_mock = mock.MagicMock()
    Captain.__db__ = db_mock
    with pytest.raises(DetaError):
        captains[1].delete()
