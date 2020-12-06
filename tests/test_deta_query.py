from odetam.query import DetaQuery, DetaQueryStatement


def test_init():
    item = DetaQuery(condition="condition", value="value")
    assert item.condition == "condition"
    assert item.value == "value"


def test_and_deta_queries():
    one = DetaQuery(condition="condition1", value="value1")
    two = DetaQuery(condition="condition2", value="value2")
    dqs = (one & two)
    assert isinstance(dqs, DetaQueryStatement)
    assert one in dqs.conditions
    assert two in dqs.conditions


def test_deta_query_and_deta_query_statement():
    one = DetaQuery(condition="condition1", value="value1")
    two = DetaQuery(condition="condition2", value="value2")
    three = DetaQuery(condition="condition3", value="value3")
    dqs = DetaQueryStatement(conditions=[one, two])
    dqs2 = (three & dqs)

    assert isinstance(dqs2, DetaQueryStatement)
    assert one in dqs2.conditions
    assert two in dqs2.conditions
    assert three in dqs2.conditions


def test_or():
    pass


def test_as_query():
    pass
