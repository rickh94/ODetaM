from unittest import mock

from odetam.query import DetaQueryList, DetaQueryStatement


def test_or(query_one, query_two, query_three, query_four):
    dql1 = DetaQueryList(conditions=[query_one, query_two])
    dql2 = DetaQueryList(conditions=[query_three, query_four])

    dql3 = dql1 | dql2

    assert isinstance(dql3, DetaQueryList)
    assert query_one in dql3.conditions
    assert query_two in dql3.conditions
    assert query_three in dql3.conditions
    assert query_four in dql3.conditions


def test_or_deta_query(query_one, query_two, query_three):
    dql1 = DetaQueryList(conditions=[query_one, query_two])

    dql2 = dql1 | query_three

    assert isinstance(dql2, DetaQueryList)
    assert query_one in dql2.conditions
    assert query_two in dql2.conditions
    assert query_three in dql2.conditions


def test_or_deta_query_statement(query_one, query_two, query_three, query_four):
    dql1 = DetaQueryList(conditions=[query_one, query_two])
    dqs = DetaQueryStatement(conditions=[query_three, query_four])

    dql2 = dql1 | dqs

    assert isinstance(dql2, DetaQueryList)
    assert query_one in dql2.conditions
    assert query_two in dql2.conditions
    assert dqs in dql2.conditions


def test_as_query(query_one, query_two):
    query_one.as_query = mock.MagicMock()
    query_one.as_query.return_value = "query_one"
    query_two.as_query = mock.MagicMock()
    query_two.as_query.return_value = "query_two"

    dql = DetaQueryList(conditions=[query_one, query_two])

    assert dql.as_query() == ["query_one", "query_two"]
    query_one.as_query.assert_called()
    query_two.as_query.assert_called()
