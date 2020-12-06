from typing import List
from unittest import mock

import pytest

from odetam.exceptions import InvalidDetaQuery
from odetam.field import DetaField
from odetam.query import DetaQuery


@pytest.fixture
def str_field():
    _mock = mock.MagicMock()
    _mock.name = "str_field"
    _mock.type_ = str
    return DetaField(_mock)


@pytest.fixture
def int_field():
    _mock = mock.MagicMock()
    _mock.name = "int_field"
    _mock.type_ = int
    return DetaField(_mock)


@pytest.fixture
def float_field():
    _mock = mock.MagicMock()
    _mock.name = "float_field"
    _mock.type_ = float
    return DetaField(_mock)


@pytest.fixture
def str_list_field():
    _mock = mock.MagicMock()
    _mock.name = "str_list_field"
    _mock.type_ = List[str]
    return DetaField(_mock)


@pytest.fixture
def int_list_field():
    _mock = mock.MagicMock()
    _mock.name = "int_list_field"
    _mock.type_ = List[int]
    return DetaField(_mock)


def test_init():
    field = DetaField("field")
    assert field.field == "field"


def test_query_expression(str_field):
    qe = str_field._query_expression("eq", "hi")
    assert isinstance(qe, DetaQuery)
    assert qe.condition == "str_field?eq"
    assert qe.value == "hi"


def test_eq(str_field):
    qe = str_field == "test"
    assert isinstance(qe, DetaQuery)
    assert qe.condition == "str_field?eq"
    assert qe.value == "test"


def test_ne(str_field):
    qe = str_field != "test"
    assert isinstance(qe, DetaQuery)
    assert qe.condition == "str_field?ne"
    assert qe.value == "test"


def test_lt(int_field):
    qe = int_field < 10
    assert isinstance(qe, DetaQuery)
    assert qe.condition == "int_field?lt"
    assert qe.value == 10


def test_gt(int_field):
    qe = int_field > 10
    assert isinstance(qe, DetaQuery)
    assert qe.condition == "int_field?gt"
    assert qe.value == 10


def test_le(int_field):
    qe = int_field <= 10
    assert isinstance(qe, DetaQuery)
    assert qe.condition == "int_field?lte"
    assert qe.value == 10


def test_ge(int_field):
    qe = int_field >= 10
    assert isinstance(qe, DetaQuery)
    assert qe.condition == "int_field?gte"
    assert qe.value == 10


def test_prefix(str_field):
    qe = str_field.prefix("test")
    assert isinstance(qe, DetaQuery)
    assert qe.condition == "str_field?pfx"
    assert qe.value == "test"


def test_prefix_wrong_type(int_field):
    with pytest.raises(InvalidDetaQuery):
        int_field.prefix("123")


def test_prefix_other_wrong_type(str_field):
    with pytest.raises(InvalidDetaQuery):
        str_field.prefix(123)


def test_range(int_field):
    qe = int_field.range(1, 5)
    assert isinstance(qe, DetaQuery)
    assert qe.condition == "int_field?r"
    assert qe.value == [1, 5]


def test_range_float(float_field):
    qe = float_field.range(1.3, 5.8)
    assert isinstance(qe, DetaQuery)
    assert qe.condition == "float_field?r"
    assert qe.value == [1.3, 5.8]


def test_range_wrong_type(str_field):
    with pytest.raises(InvalidDetaQuery):
        str_field.range(1, 5)


def test_range_wrong_lower(int_field):
    with pytest.raises(InvalidDetaQuery):
        int_field.range("1", 5)


def test_range_wrong_upper(int_field):
    with pytest.raises(InvalidDetaQuery):
        int_field.range(1, "5")


def test_range_upper_less_than_lower(int_field):
    with pytest.raises(InvalidDetaQuery):
        int_field.range(5, 1)


def test_contains(str_field):
    qe = str_field.contains("test")
    assert isinstance(qe, DetaQuery)
    assert qe.condition == "str_field?contains"
    assert qe.value == "test"


def test_contains_list(str_list_field):
    qe = str_list_field.contains("test")
    assert isinstance(qe, DetaQuery)
    assert qe.condition == "str_list_field?contains"
    assert qe.value == "test"


def test_contains_wrong_type(int_field):
    with pytest.raises(InvalidDetaQuery):
        int_field.contains("test")


def test_contains_arg_wrong_type(str_field):
    with pytest.raises(InvalidDetaQuery):
        str_field.contains(123)


def test_contains_wrong_list_type(int_list_field):
    with pytest.raises(InvalidDetaQuery):
        int_list_field.contains("test")


def test_not_contains(str_field):
    qe = str_field.not_contains("test")
    assert isinstance(qe, DetaQuery)
    assert qe.condition == "str_field?not_contains"
    assert qe.value == "test"


def test_not_contains_list(str_list_field):
    qe = str_list_field.not_contains("test")
    assert isinstance(qe, DetaQuery)
    assert qe.condition == "str_list_field?not_contains"
    assert qe.value == "test"


def test_not_contains_wrong_type(int_field):
    with pytest.raises(InvalidDetaQuery):
        int_field.not_contains("test")


def test_not_contains_wrong_list_type(int_list_field):
    with pytest.raises(InvalidDetaQuery):
        int_list_field.not_contains("test")

def test_not_contains_arg_wrong_type(str_field):
    with pytest.raises(InvalidDetaQuery):
        str_field.not_contains(123)
