import datetime
from typing import List
from unittest import mock

import pytest
from pydantic import BaseModel

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


@pytest.fixture
def date_field():
    _mock = mock.MagicMock()
    _mock.name = "date_field"
    _mock.type_ = datetime.date
    return DetaField(_mock)


@pytest.fixture
def datetime_field():
    _mock = mock.MagicMock()
    _mock.name = "datetime_field"
    _mock.type_ = datetime.datetime
    return DetaField(_mock)


@pytest.fixture
def time_field():
    _mock = mock.MagicMock()
    _mock.name = "time_field"
    _mock.type_ = datetime.time
    return DetaField(_mock)


@pytest.fixture
def a_date():
    return datetime.date(2021, 1, 1)


@pytest.fixture
def a_datetime():
    return datetime.datetime(2021, 1, 1, 1, 5, 2)


@pytest.fixture
def a_time():
    return datetime.time(0, 10, 0, 40)


def test_init():
    field = DetaField("field")
    assert field.field == "field"


def test_query_expression(str_field):
    qe = str_field._query_expression("eq", "hi")
    assert isinstance(qe, DetaQuery)
    assert qe.condition == "str_field?eq"
    assert qe.value == "hi"


def test_query_expression_nested_field():
    class Data(BaseModel):
        name: str

    inner_field = mock.MagicMock()
    inner_field.name = "data"
    inner_field.type_ = Data
    field = DetaField(field=inner_field)
    qe = field._query_expression("ne", Data(name="hi"))

    assert isinstance(qe, DetaQuery)
    assert qe.condition == "data?ne"
    assert qe.value == {"name": "hi"}


def test_eq(str_field):
    qe = str_field == "test"
    assert isinstance(qe, DetaQuery)
    assert qe.condition == "str_field"
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
    with pytest.raises(TypeError):
        str_field.range("1", "5")


def test_range_wrong_lower(int_field):
    with pytest.raises(TypeError):
        int_field.range("1", 5)


def test_range_wrong_upper(int_field):
    with pytest.raises(TypeError):
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


def test_query_eq_date(date_field, a_date):
    qe = date_field == a_date
    assert isinstance(qe, DetaQuery)
    assert qe.condition == "date_field"
    assert qe.value == int(a_date.strftime("%Y%d%m"))


def test_query_ne_date(date_field, a_date):
    qe = date_field != a_date
    assert isinstance(qe, DetaQuery)
    assert qe.condition == "date_field?ne"
    assert qe.value == int(a_date.strftime("%Y%d%m"))


def test_query_lt_date(date_field, a_date):
    qe = date_field < a_date
    assert isinstance(qe, DetaQuery)
    assert qe.condition == "date_field?lt"
    assert qe.value == int(a_date.strftime("%Y%d%m"))


def test_query_gt_date(date_field, a_date):
    qe = date_field > a_date
    assert isinstance(qe, DetaQuery)
    assert qe.condition == "date_field?gt"
    assert qe.value == int(a_date.strftime("%Y%d%m"))


def test_query_le_date(date_field, a_date):
    qe = date_field <= a_date
    assert isinstance(qe, DetaQuery)
    assert qe.condition == "date_field?lte"
    assert qe.value == int(a_date.strftime("%Y%d%m"))


def test_query_ge_date(date_field, a_date):
    qe = date_field >= a_date
    assert isinstance(qe, DetaQuery)
    assert qe.condition == "date_field?gte"
    assert qe.value == int(a_date.strftime("%Y%d%m"))


def test_date_range(date_field, a_date):
    another_date = datetime.date(2021, 4, 2)
    qe = date_field.range(a_date, another_date)
    assert isinstance(qe, DetaQuery)
    assert qe.condition == "date_field?r"
    assert qe.value == [
        int(a_date.strftime("%Y%m%d")),
        int(another_date.strftime("%Y%m%d")),
    ]


def test_query_eq_datetime(datetime_field, a_datetime):
    qe = datetime_field == a_datetime
    assert isinstance(qe, DetaQuery)
    assert qe.condition == "datetime_field"
    assert qe.value == a_datetime.timestamp()


def test_query_ne_datetime(datetime_field, a_datetime):
    qe = datetime_field != a_datetime
    assert isinstance(qe, DetaQuery)
    assert qe.condition == "datetime_field?ne"
    assert qe.value == a_datetime.timestamp()


def test_query_lt_datetime(datetime_field, a_datetime):
    qe = datetime_field < a_datetime
    assert isinstance(qe, DetaQuery)
    assert qe.condition == "datetime_field?lt"
    assert qe.value == a_datetime.timestamp()


def test_query_gt_datetime(datetime_field, a_datetime):
    qe = datetime_field > a_datetime
    assert isinstance(qe, DetaQuery)
    assert qe.condition == "datetime_field?gt"
    assert qe.value == a_datetime.timestamp()


def test_query_le_datetime(datetime_field, a_datetime):
    qe = datetime_field <= a_datetime
    assert isinstance(qe, DetaQuery)
    assert qe.condition == "datetime_field?lte"
    assert qe.value == a_datetime.timestamp()


def test_query_ge_datetime(datetime_field, a_datetime):
    qe = datetime_field >= a_datetime
    assert isinstance(qe, DetaQuery)
    assert qe.condition == "datetime_field?gte"
    assert qe.value == a_datetime.timestamp()


def test_datetime_range(datetime_field, a_datetime):
    another_datetime = datetime.datetime(2021, 4, 2, 1, 2, 4)
    qe = datetime_field.range(a_datetime, another_datetime)
    assert isinstance(qe, DetaQuery)
    assert qe.condition == "datetime_field?r"
    assert qe.value == [
        a_datetime.timestamp(),
        another_datetime.timestamp(),
    ]


def test_query_eq_time(time_field, a_time):
    qe = time_field == a_time
    assert isinstance(qe, DetaQuery)
    assert qe.condition == "time_field"
    assert qe.value == int(a_time.strftime("%H%M%S%f"))


def test_query_ne_time(time_field, a_time):
    qe = time_field != a_time
    assert isinstance(qe, DetaQuery)
    assert qe.condition == "time_field?ne"
    assert qe.value == int(a_time.strftime("%H%M%S%f"))


def test_query_lt_time(time_field, a_time):
    qe = time_field < a_time
    assert isinstance(qe, DetaQuery)
    assert qe.condition == "time_field?lt"
    assert qe.value == int(a_time.strftime("%H%M%S%f"))


def test_query_gt_time(time_field, a_time):
    qe = time_field > a_time
    assert isinstance(qe, DetaQuery)
    assert qe.condition == "time_field?gt"
    assert qe.value == int(a_time.strftime("%H%M%S%f"))


def test_query_le_time(time_field, a_time):
    qe = time_field <= a_time
    assert isinstance(qe, DetaQuery)
    assert qe.condition == "time_field?lte"
    assert qe.value == int(a_time.strftime("%H%M%S%f"))


def test_query_ge_time(time_field, a_time):
    qe = time_field >= a_time
    assert isinstance(qe, DetaQuery)
    assert qe.condition == "time_field?gte"
    assert qe.value == int(a_time.strftime("%H%M%S%f"))


def test_time_range(time_field, a_time):
    another_time = datetime.time(1, 2, 4, 2)
    qe = time_field.range(a_time, another_time)
    assert isinstance(qe, DetaQuery)
    assert qe.condition == "time_field?r"
    assert qe.value == [
        int(a_time.strftime("%H%M%S%f")),
        int(another_time.strftime("%H%M%S%f")),
    ]


def test_compare_wrong_type(int_field):
    with pytest.raises(TypeError):
        int_field._check_type([1, 2, 3])
    with pytest.raises(TypeError):
        qe = int_field == "5"
