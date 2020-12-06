import pytest

from odetam.exceptions import InvalidDetaQuery
from odetam.query import DetaQueryStatement, DetaQueryList


def test_and(query_one, query_two, query_three, query_four):
    dqs1 = DetaQueryStatement(conditions=[query_one, query_two])
    dqs2 = DetaQueryStatement(conditions=[query_three, query_four])

    dqs3 = dqs1 & dqs2

    assert isinstance(dqs3, DetaQueryStatement)
    assert query_one in dqs3.conditions
    assert query_two in dqs3.conditions
    assert query_three in dqs3.conditions
    assert query_four in dqs3.conditions


def test_and_deta_query(query_one, query_two, query_three):
    dqs1 = DetaQueryStatement(conditions=[query_one, query_two])

    dqs2 = dqs1 & query_three

    assert isinstance(dqs2, DetaQueryStatement)
    assert query_one in dqs2.conditions
    assert query_two in dqs2.conditions
    assert query_three in dqs2.conditions


def test_and_deta_query_list_raises(query_one, query_two, query_three, query_four):
    dqs1 = DetaQueryStatement(conditions=[query_one, query_two])
    dql1 = DetaQueryList(conditions=[query_three, query_four])
    with pytest.raises(InvalidDetaQuery):
        dqs1 & dql1


def test_or(query_one, query_two, query_three, query_four):
    dqs1 = DetaQueryStatement(conditions=[query_one, query_two])
    dqs2 = DetaQueryStatement(conditions=[query_three, query_four])

    dql = dqs1 | dqs2

    assert isinstance(dql, DetaQueryList)
    assert dqs1 in dql.conditions
    assert dqs2 in dql.conditions


def test_or_deta_query_list(query_one, query_two, query_three, query_four):
    dqs = DetaQueryStatement(conditions=[query_one, query_two])
    dql1 = DetaQueryList(conditions=[query_three, query_four])

    dql2 = dqs | dql1

    assert isinstance(dql2, DetaQueryList)
    assert dqs in dql2.conditions
    assert query_three in dql2.conditions
    assert query_four in dql2.conditions


def test_as_query(query_one, query_two):
    dqs = DetaQueryStatement(conditions=[query_one, query_two])
    assert dqs.as_query() == {
        "condition1": "value1",
        "condition2": "value2",
    }
