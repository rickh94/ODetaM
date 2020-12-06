import pytest

from odetam.query import DetaQuery


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
