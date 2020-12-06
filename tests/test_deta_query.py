from odetam.query import DetaQuery, DetaQueryStatement, DetaQueryList


def test_init():
    item = DetaQuery(condition="condition", value="value")
    assert item.condition == "condition"
    assert item.value == "value"


def test_and_deta_queries(query_one, query_two):
    dqs = query_one & query_two
    assert isinstance(dqs, DetaQueryStatement)
    assert query_one in dqs.conditions
    assert query_two in dqs.conditions


def test_deta_query_and_deta_query_statement(query_one, query_two, query_three):
    dqs = DetaQueryStatement(conditions=[query_one, query_two])
    dqs2 = query_three & dqs

    assert isinstance(dqs2, DetaQueryStatement)
    assert query_one in dqs2.conditions
    assert query_two in dqs2.conditions
    assert query_three in dqs2.conditions


def test_or(query_one, query_two):
    dql = query_one | query_two
    assert isinstance(dql, DetaQueryList)
    assert query_one in dql.conditions
    assert query_two in dql.conditions


def test_deta_query_or_deta_query_list(query_one, query_two, query_three):
    dql1 = DetaQueryList(conditions=[query_one, query_two])
    dql2 = query_three | dql1
    assert isinstance(dql2, DetaQueryList)
    assert query_one in dql2.conditions
    assert query_two in dql2.conditions
    assert query_three in dql2.conditions


def test_as_query(query_one, query_two, query_three):
    assert query_one.as_query() == {"condition1": "value1"}
