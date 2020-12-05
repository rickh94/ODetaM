import datetime
import os
from typing import List
from unittest import mock

import pytest

from odetam import __version__, DetaModel
from odetam.exceptions import NoProjectKey, ItemNotFound
from odetam.field import DetaField


@pytest.fixture
def Captain():
    class _Captain(DetaModel):
        name: str
        joined: datetime.date
        ships: List[str]

    return _Captain


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


def test_get_all(Captain):
    db_mock = mock.MagicMock()
    Captain.__db__ = db_mock
    mock_records = [
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

    def _mock_fetch():
        yield mock_records

    db_mock.fetch = _mock_fetch

    records = Captain.get_all()
    assert len(records) == len(mock_records)
    assert isinstance(records[0], Captain)
    assert isinstance(records[1], Captain)
    assert Captain.parse_obj(mock_records[0]) in records
    assert Captain.parse_obj(mock_records[1]) in records


def test_query(Captain):
    mock_records = [
        {
            "name": "Benjamin Sisko",
            "joined": "2350-01-01",
            "ships": ["Deep Space 9", "Defiant"],
            "key": "key2",
        }
    ]

    def _mock_fetch(query_statement):
        assert query_statement["example"] == "query"

        yield mock_records

    Captain.__db__.fetch = _mock_fetch
    mock_query_statement = mock.MagicMock()
    mock_query_statement.as_query.return_value = {"example": "query"}
    results = Captain.query(mock_query_statement)

    assert len(results) == 1
    assert Captain.parse_obj(mock_records[0]) in results


def test_delete_key(Captain):
    Captain.__db__ = mock.MagicMock()

    Captain.delete_key("testkey")
    Captain.__db__.delete.assert_called_with("testkey")


def test_put_many():
    pass


def test_save_new():
    pass


def test_save_existing():
    pass


def test_delete():
    pass
