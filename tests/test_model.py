import datetime
import ipaddress
import os
import random
from typing import List, Optional
from unittest import mock

import deta
import pytest
import ujson
from faker import Faker
from pydantic import EmailStr

from odetam import __version__, DetaModel
from odetam.exceptions import NoProjectKey, ItemNotFound, DetaError
from odetam.field import DetaField


@pytest.fixture
def Basic(monkeypatch):
    monkeypatch.setenv("PROJECT_KEY", "123_123")

    class _Basic(DetaModel):
        name: str

    _Basic._db = mock.MagicMock()
    return _Basic


# noinspection PyPep8Naming
@pytest.fixture
def Captain(monkeypatch):
    monkeypatch.setenv("PROJECT_KEY", "123_123")

    class _Captain(DetaModel):
        name: str
        joined: datetime.date
        ships: List[str]

    _Captain._db = mock.MagicMock()
    return _Captain


@pytest.fixture
def Appointment(monkeypatch):
    monkeypatch.setenv("PROJECT_KEY", "123_123")

    class _Appointment(DetaModel):
        name: str
        at: datetime.time

    _Appointment._db = mock.MagicMock()
    return _Appointment


@pytest.fixture
def Event(monkeypatch):
    monkeypatch.setenv("PROJECT_KEY", "123_123")

    class _Event(DetaModel):
        name: str
        at: datetime.datetime

    _Event._db = mock.MagicMock()
    return _Event


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
            "joined": 22520101,
            "ships": ["Enterprise", "Enterprise-A"],
        },
        {
            "name": "Benjamin Sisko",
            "joined": 23500101,
            "ships": ["Deep Space 9", "Defiant"],
        },
    ]


@pytest.fixture
def captains_with_keys_list(captains_list):
    return [
        {
            "name": "James T. Kirk",
            "joined": 22520101,
            "ships": ["Enterprise", "Enterprise-A"],
            "key": "key1",
        },
        {
            "name": "Benjamin Sisko",
            "joined": 23500101,
            "ships": ["Deep Space 9", "Defiant"],
            "key": "key2",
        },
    ]


# noinspection PyPep8Naming
@pytest.fixture
def UnrulyModel(monkeypatch):
    monkeypatch.setenv("PROJECT_KEY", "123_123")

    class _UnrulyModel(DetaModel):
        name: Optional[str]
        email: EmailStr
        ips: List[ipaddress.IPv4Address]

    return _UnrulyModel


# noinspection PyPep8Naming
@pytest.fixture
def HasOptional(monkeypatch):
    monkeypatch.setenv("PROJECT_KEY", "123_123")

    class _HasOptional(DetaModel):
        name: Optional[str]

    return _HasOptional


def test_deta_meta_model_class_creates_db_lazily(monkeypatch):
    deta_mock = mock.MagicMock()
    instance_mock = mock.MagicMock()
    deta_mock.return_value = instance_mock
    db = mock.MagicMock()
    instance_mock.Base.return_value = db
    monkeypatch.setattr("odetam.model.Deta", deta_mock)

    class ObjectExample(DetaModel):
        name: str

    ObjectExample(name="hi").save()

    deta_mock.assert_called_with("123_123")
    instance_mock.Base.assert_called_with("object_example")
    assert ObjectExample.__db_name__ == "object_example"
    assert ObjectExample.__db__ == db


def test_deta_meta_model_raises_error_with_no_project_key():
    os.environ["PROJECT_KEY"] = ""
    with pytest.raises(NoProjectKey):

        class ObjectExample(DetaModel):
            pass

        ObjectExample().save()

    os.environ["PROJECT_KEY"] = "123_123"


def test_deta_meta_model_assigns_fields():
    class ObjectExample(DetaModel):
        name: str
        age: int

    assert isinstance(ObjectExample.name, DetaField)
    assert isinstance(ObjectExample.age, DetaField)


def test_basic_get(Basic):
    Basic._db.get.return_value = {"name": "test", "key": "key2"}
    new_thing = Basic.get(key="key2")

    Basic._db.get.assert_called_with("key2")
    assert new_thing.key == "key2"
    assert new_thing.name == "test"


def test_get_not_found_raises_error(Captain):
    Captain._db.get.return_value = None
    with pytest.raises(ItemNotFound):
        Captain.get("key1")


def test_get_all(Captain, captains_with_keys_list):
    def _mock_fetch():
        return deta.base.FetchResponse(
            count=len(captains_with_keys_list), last=None, items=captains_with_keys_list
        )

    Captain._db.fetch = _mock_fetch

    records = Captain.get_all()
    assert len(records) == len(captains_with_keys_list)
    for record in records:
        assert isinstance(record, Captain)
    for captain in captains_with_keys_list:
        assert Captain._deserialize(captain) in records


def test_query(Captain, captains_with_keys_list):
    def _mock_fetch(query_statement):
        assert query_statement["example"] == "query"
        return deta.base.FetchResponse(
            count=1, last=None, items=captains_with_keys_list[1:2]
        )

    Captain._db.fetch = _mock_fetch
    mock_query_statement = mock.MagicMock()
    mock_query_statement.as_query.return_value = {"example": "query"}

    results = Captain.query(mock_query_statement)

    assert len(results) == 1
    assert Captain._deserialize(captains_with_keys_list[1]) in results


def test_delete_key(Captain):
    Captain._db = mock.MagicMock()

    Captain.delete_key("testkey")
    Captain._db.delete.assert_called_with("testkey")


def test_put_many(Captain, captains_list, captains_with_keys_list, captains):
    Captain._db.put_many.return_value = {
        "processed": {"items": captains_with_keys_list}
    }
    results = Captain.put_many(captains)

    Captain._db.put_many.assert_called_with(captains_list)

    assert len(results) == len(captains)
    for captain in captains_with_keys_list:
        assert Captain._deserialize(captain) in results


def test_put_more_than_25(Captain, make_bunch_of_random_captains):
    """Test put many is correctly batched for lists of items larger than
    25.
    """
    captains, captain_data, captain_data_with_keys = make_bunch_of_random_captains(
        Captain, 52
    )
    # the three calls should have batches of 25 and the api will return process data
    # in this form
    Captain._db.put_many.side_effect = [
        {"processed": {"items": captain_data_with_keys[0:25]}},
        {"processed": {"items": captain_data_with_keys[25:50]}},
        {"processed": {"items": captain_data_with_keys[50:]}},
    ]
    results = Captain.put_many(captains)

    # Two batches of 25, and the remaining records
    assert Captain._db.put_many.call_count == 3
    # put_many should have been called with each of the batches
    Captain._db.put_many.assert_any_call(captain_data[0:25])
    Captain._db.put_many.assert_any_call(captain_data[25:50])
    Captain._db.put_many.assert_any_call(captain_data[50:])

    # resulting data should have been flattened and serialized, and have keys
    # added from database result
    for item in captain_data_with_keys:
        assert Captain._deserialize(item) in results


def test_basic_save(Basic):
    Basic._db = mock.MagicMock()
    data = {"name": "test"}
    Basic._db.put.return_value = {**data, "key": "key1"}
    new_thing = Basic(name="test")
    new_thing.save()

    Basic._db.put.assert_called_with(data)
    assert new_thing.key == "key1"


def test_get_converts_date_back(Captain):
    Captain._db.get.return_value = {
        "name": "Jean-Luc Picard",
        "joined": 23230101,
        "ships": ["Enterprise-D", "Enterprise-E", "La Sirena"],
        "key": "key5",
    }
    picard = Captain.get("key5")

    Captain._db.get.assert_called_with("key5")

    assert picard.name == "Jean-Luc Picard"
    assert picard.joined == datetime.date(2323, 1, 1)
    assert picard.ships == ["Enterprise-D", "Enterprise-E", "La Sirena"]
    assert picard.key == "key5"


def test_save_converts_date(Captain):
    data = {"name": "Saru", "joined": 22490101, "ships": ["Discovery"]}
    Captain._db.put.return_value = {**data, "key": "key8"}
    saru = Captain(name=data["name"], ships=["Discovery"], joined="2249-01-01")
    saru.save()

    Captain._db.put.assert_called_with(data)
    assert saru.key == "key8"


def test_save_converts_datetime(Event):
    at = datetime.datetime(2021, 8, 1, 20, 26, 51, 737609)
    Event._db.put.return_value = {
        "at": at.timestamp(),
        "name": "Concert",
        "key": "key8",
    }
    concert = Event(name="Concert", at=at)
    concert.save()

    Event._db.put.assert_called_with({"name": "Concert", "at": at.timestamp()})
    assert concert.key == "key8"


def test_save_converts_time(Appointment):
    at = datetime.time(12, 11, 1, 12)
    Appointment._db.put.return_value = {
        "at": 1211101000012,
        "name": "Concert",
        "key": "key8",
    }
    doctor = Appointment(name="Doctor", at=at)
    doctor.save()

    Appointment._db.put.assert_called_with({"name": "Doctor", "at": 121101000012})
    assert doctor.key == "key8"


def test_get_converts_time_back(Appointment):
    Appointment._db.get.return_value = {
        "key": "key5",
        "name": "doctor",
        "at": 121101000012,
    }

    doctor = Appointment.get("key5")

    Appointment._db.get.assert_called_with("key5")

    assert doctor.name == "doctor"
    assert doctor.at == datetime.time(12, 11, 1, 12)


def test_get_converts_datetime_back(Event):
    at = datetime.datetime(2021, 8, 1, 20, 26, 51, 737609)
    Event._db.get.return_value = {
        "at": at.timestamp(),
        "name": "Concert",
        "key": "key8",
    }
    concert = Event.get("key8")

    Event._db.get.assert_called_with("key8")

    assert concert.name == "Concert"
    assert concert.key == "key8"
    assert concert.at == at


def test_save_existing(Captain):
    data = {
        "name": "Katheryn Janeway",
        "joined": 23600101,
        "ships": ["Voyager"],
        "key": "key25",
    }
    Captain._db.put.return_value = data
    janeway = Captain(
        name=data["name"], ships=data["ships"], key=data["key"], joined="2360-01-01"
    )
    janeway.save()
    Captain._db.put.assert_called_with(data)
    assert janeway.key == "key25"


def test_delete(captains_list, Captain):
    captain = Captain.parse_obj(captains_list[0])
    captain.key = "key22"
    captain.delete()
    Captain._db.delete.assert_called_with("key22")
    assert captain.key is None


def test_delete_no_key(captains):
    with pytest.raises(DetaError):
        captains[1].delete()


def test_serialize_optional_attribute(HasOptional):
    thing = HasOptional(name=None)

    assert thing._serialize() == {"name": None}


def test_serialize_weird_attributes(UnrulyModel):
    unruly = UnrulyModel(email="test@example.com", ips=["192.168.1.1", "10.0.1.1"])

    assert unruly._serialize() == {
        "name": None,
        "email": "test@example.com",
        "ips": ["192.168.1.1", "10.0.1.1"],
    }


def test_deserialize_optional_attribute(HasOptional):
    thing = HasOptional._deserialize({})

    print(HasOptional.name.field.type_)

    assert thing.name is None


def test_deserialize_weird_attributes(UnrulyModel):
    unruly = UnrulyModel._deserialize(
        {
            "name": None,
            "email": "test@example.com",
            "ips": ["192.168.1.1", "10.0.1.1"],
        }
    )

    assert unruly.name is None
    assert unruly.email == "test@example.com"
    assert unruly.ips == [
        ipaddress.IPv4Address("192.168.1.1"),
        ipaddress.IPv4Address("10.0.1.1"),
    ]
