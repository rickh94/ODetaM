import asyncio
import datetime
import ipaddress
import os
from typing import List, Optional
from unittest import mock

import pytest
from pydantic import EmailStr

from odetam.async_model import AsyncDetaModel
from odetam.exceptions import ItemNotFound, DetaError
from odetam.field import DetaField


@pytest.fixture
def Basic(monkeypatch):
    monkeypatch.setenv("DETA_PROJECT_KEY", "123_123")

    class _Basic(AsyncDetaModel):
        name: str

    _Basic._db = mock.MagicMock()
    return _Basic


# noinspection PyPep8Naming
@pytest.fixture
def Captain(monkeypatch):
    monkeypatch.setenv("DETA_PROJECT_KEY", "123_123")

    class _Captain(AsyncDetaModel):
        name: str
        joined: datetime.date
        ships: List[str]

    _Captain._db = mock.MagicMock()
    return _Captain


@pytest.fixture
def Appointment(monkeypatch):
    monkeypatch.setenv("DETA_PROJECT_KEY", "123_123")

    class _Appointment(AsyncDetaModel):
        name: str
        at: datetime.time

    _Appointment._db = mock.MagicMock()
    return _Appointment


@pytest.fixture
def Event(monkeypatch):
    monkeypatch.setenv("DETA_PROJECT_KEY", "123_123")

    class _Event(AsyncDetaModel):
        name: str
        at: datetime.datetime

    _Event._db = mock.MagicMock()
    return _Event


@pytest.fixture
def FakeResult():
    class _FakeResult:
        def __init__(self, items, last=None):
            self.items = items
            self.last = last

    return _FakeResult


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
    monkeypatch.setenv("DETA_PROJECT_KEY", "123_123")

    class _UnrulyModel(AsyncDetaModel):
        name: Optional[str]
        email: EmailStr
        ips: List[ipaddress.IPv4Address]

    return _UnrulyModel


# noinspection PyPep8Naming
@pytest.fixture
def HasOptional(monkeypatch):
    monkeypatch.setenv("DETA_PROJECT_KEY", "123_123")

    class _HasOptional(AsyncDetaModel):
        name: Optional[str]

    return _HasOptional


def future_with(value):
    fut = asyncio.Future()
    fut.set_result(value)
    return fut


def put_returns_items(items):
    if not isinstance(items, list):
        items = [items]
    return {"processed": {"items": items}}


@pytest.mark.asyncio
async def test_async_deta_meta_model_class_creates_db_lazily(monkeypatch):
    monkeypatch.setenv("DETA_PROJECT_KEY", "123_123")

    monkeypatch.setenv("DETA_PROJECT_KEY", "123_123")
    db_mock = mock.MagicMock()
    db_instance_mock = mock.MagicMock()
    db_instance_mock.put.return_value = future_with(
        {"key": "testkey", "name": "object_example"}
    )
    db_mock.return_value = db_instance_mock
    monkeypatch.setattr("odetam.async_model.AsyncBase", db_mock)

    class ObjectExample(AsyncDetaModel):
        name: str

    await ObjectExample(name="hi").save()

    db_mock.assert_called_with("object_example")
    assert ObjectExample.__db_name__ == "object_example"
    assert ObjectExample.__db__ == db_instance_mock


@pytest.mark.asyncio
async def test_async_deta_meta_model_raises_error_with_no_DETA_PROJECT_KEY():
    os.environ["DETA_PROJECT_KEY"] = ""
    with pytest.raises(AssertionError):

        class ObjectExample(AsyncDetaModel):
            pass

        await ObjectExample().save()

    os.environ["DETA_PROJECT_KEY"] = "123_123"


def test_async_deta_meta_model_assigns_fields():
    class ObjectExample(AsyncDetaModel):
        name: str
        age: int

    assert isinstance(ObjectExample.name, DetaField)
    assert isinstance(ObjectExample.age, DetaField)


@pytest.mark.asyncio
async def test_basic_async_get(Basic):
    Basic._db.get.return_value = future_with({"name": "test", "key": "key2"})
    new_thing = await Basic.get(key="key2")

    Basic._db.get.assert_called_with("key2")
    assert new_thing.key == "key2"
    assert new_thing.name == "test"


@pytest.mark.asyncio
async def test_async_get_not_found_raises_error(Captain):
    Captain._db.get.return_value = future_with(None)
    with pytest.raises(ItemNotFound):
        await Captain.get("key1")


@pytest.mark.asyncio
async def test_async_get_all(Captain, captains_with_keys_list, FakeResult):
    async def _mock_query():
        return FakeResult(captains_with_keys_list)

    Captain._db.fetch = _mock_query

    records = await Captain.get_all()
    assert len(records) == len(captains_with_keys_list)
    for record in records:
        assert isinstance(record, Captain)
    for captain in captains_with_keys_list:
        assert Captain._deserialize(captain) in records


# FIXME: test_async_get_all does NOT test pagination of very large sets.


@pytest.mark.asyncio
async def test_async_query(Captain, captains_with_keys_list, FakeResult):
    async def _mock_async_fetch(query_statement):
        assert query_statement["example"] == "query"
        return FakeResult(captains_with_keys_list[1:2])

    Captain._db.fetch = _mock_async_fetch
    mock_query_statement = mock.MagicMock()
    mock_query_statement.as_query.return_value = {"example": "query"}

    results = await Captain.query(mock_query_statement)

    assert len(results) == 1
    assert Captain._deserialize(captains_with_keys_list[1]) in results


@pytest.mark.asyncio
async def test_async_delete_key(Captain):
    Captain._db = mock.MagicMock()
    Captain._db.delete.return_value = future_with(None)

    await Captain.delete_key("testkey")
    Captain._db.delete.assert_called_with("testkey")


@pytest.mark.asyncio
async def test_async_put_many(
    Captain, captains_list, captains_with_keys_list, captains
):
    Captain._db.put_many.return_value = future_with(
        put_returns_items(captains_with_keys_list)
    )
    results = await Captain.put_many(captains)

    Captain._db.put_many.assert_called_with(captains_list)

    assert len(results) == len(captains)
    for captain in captains_with_keys_list:
        assert Captain._deserialize(captain) in results


@pytest.mark.asyncio
async def test_async_put_more_than_25(Captain, make_bunch_of_random_captains):
    """Test put many is correctly batched for lists of items larger than
    25.
    """
    captains, captain_data, captain_data_with_keys = make_bunch_of_random_captains(
        Captain, 52
    )
    Captain._db.put_many.side_effect = [
        future_with({"processed": {"items": captain_data_with_keys[0:25]}}),
        future_with({"processed": {"items": captain_data_with_keys[25:50]}}),
        future_with({"processed": {"items": captain_data_with_keys[50:]}}),
    ]
    results = await Captain.put_many(captains)

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


@pytest.mark.asyncio
async def test_async_basic_save(Basic):
    Basic._db = mock.MagicMock()
    data = {"name": "test"}
    Basic._db.put.return_value = future_with({**data, "key": "key1"})
    new_thing = Basic(name="test")
    await new_thing.save()

    Basic._db.put.assert_called_with(data)
    assert new_thing.key == "key1"


@pytest.mark.asyncio
async def test_async_get_converts_date_back(Captain):
    Captain._db.get.return_value = future_with(
        {
            "name": "Jean-Luc Picard",
            "joined": 23230101,
            "ships": ["Enterprise-D", "Enterprise-E", "La Sirena"],
            "key": "key5",
        }
    )
    picard = await Captain.get("key5")

    Captain._db.get.assert_called_with("key5")

    assert picard.name == "Jean-Luc Picard"
    assert picard.joined == datetime.date(2323, 1, 1)
    assert picard.ships == ["Enterprise-D", "Enterprise-E", "La Sirena"]
    assert picard.key == "key5"


@pytest.mark.asyncio
async def test_async_save_converts_date(Captain):
    data = {"name": "Saru", "joined": 22490101, "ships": ["Discovery"]}
    Captain._db.put.return_value = future_with({**data, "key": "key8"})
    saru = Captain(name=data["name"], ships=["Discovery"], joined="2249-01-01")
    await saru.save()

    Captain._db.put.assert_called_with(data)
    assert saru.key == "key8"


@pytest.mark.asyncio
async def test_async_save_converts_datetime(Event):
    at = datetime.datetime(2021, 8, 1, 20, 26, 51, 737609)
    Event._db.put.return_value = future_with(
        {
            "at": at.timestamp(),
            "name": "Concert",
            "key": "key8",
        }
    )
    concert = Event(name="Concert", at=at)
    await concert.save()

    Event._db.put.assert_called_with({"name": "Concert", "at": at.timestamp()})
    assert concert.key == "key8"


@pytest.mark.asyncio
async def test_async_save_converts_time(Appointment):
    at = datetime.time(12, 11, 1, 12)
    Appointment._db.put.return_value = future_with(
        {
            "at": 1211101000012,
            "name": "Concert",
            "key": "key8",
        }
    )
    doctor = Appointment(name="Doctor", at=at)
    await doctor.save()

    Appointment._db.put.assert_called_with({"name": "Doctor", "at": 121101000012})
    assert doctor.key == "key8"


@pytest.mark.asyncio
async def test_async_get_converts_time_back(Appointment):
    Appointment._db.get.return_value = future_with(
        {
            "key": "key5",
            "name": "doctor",
            "at": 121101000012,
        }
    )

    doctor = await Appointment.get("key5")

    Appointment._db.get.assert_called_with("key5")

    assert doctor.name == "doctor"
    assert doctor.at == datetime.time(12, 11, 1, 12)


@pytest.mark.asyncio
async def test_async_get_converts_datetime_back(Event):
    at = datetime.datetime(2021, 8, 1, 20, 26, 51, 737609)
    Event._db.get.return_value = future_with(
        {
            "at": at.timestamp(),
            "name": "Concert",
            "key": "key8",
        }
    )
    concert = await Event.get("key8")

    Event._db.get.assert_called_with("key8")

    assert concert.name == "Concert"
    assert concert.key == "key8"
    assert concert.at == at


@pytest.mark.asyncio
async def test_async_save_existing(Captain):
    data = {
        "name": "Katheryn Janeway",
        "joined": 23600101,
        "ships": ["Voyager"],
        "key": "key25",
    }
    Captain._db.put.return_value = future_with(data)
    janeway = Captain(
        name=data["name"], ships=data["ships"], key=data["key"], joined="2360-01-01"
    )
    await janeway.save()
    Captain._db.put.assert_called_with(data)
    assert janeway.key == "key25"


@pytest.mark.asyncio
async def test_delete(captains_list, Captain):
    Captain._db.delete.return_value = future_with(None)
    captain = Captain.parse_obj(captains_list[0])
    captain.key = "key22"
    await captain.delete()
    Captain._db.delete.assert_called_with("key22")
    assert captain.key is None


@pytest.mark.asyncio
async def test_delete_no_key(captains, Captain):
    Captain._db.delete.return_value = future_with(None)
    with pytest.raises(DetaError):
        await captains[1].delete()
