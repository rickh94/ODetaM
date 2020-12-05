import datetime
from typing import List
from unittest import mock

import pytest

from odetam import __version__, DetaModel


@pytest.fixture
def Captain():
    class _Captain(DetaModel):
        name: str
        joined: datetime.date
        ships: List[str]

    return _Captain


def test_version():
    assert __version__ == "0.1.0"


def test_deta_model_meta_class_creates_db(monkeypatch):
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


def test_get(Captain, monkeypatch):
    db_mock = mock.MagicMock()
    Captain.__db__ = db_mock
    db_mock.get.return_value = {
        "name": "Jean-Luc Picard",
        "joined": "2323-01-01",
        "ships": ["Enterprise-D", "Enterprise-E", "La Sirena"],
        "key": "key5"
    }
    picard = Captain.get("key5")

    db_mock.get.assert_called_with("key5")

    assert picard.name == "Jean-Luc Picard"
    assert picard.joined == datetime.date(2323, 1, 1)
    assert picard.ships == ["Enterprise-D", "Enterprise-E", "La Sirena"]
    assert picard.key == "key5"


def test_get_all():
    pass


def test_query():
    pass


def test_delete_key():
    pass


def test_put_many():
    pass


def test_save_new():
    pass


def test_save_existing():
    pass


def test_delete():
    pass
