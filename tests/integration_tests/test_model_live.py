import datetime
import os

import pytest
from faker import Faker

from odetam import DetaModel
from odetam.exceptions import ItemNotFound

INTEGRATION_TEST_KEY = os.getenv("INTEGRATION_TEST_KEY")


def reduce_items_to_keys(items):
    for item in items:
        yield item.key


@pytest.fixture
def IntegrationBasic(monkeypatch, unique_test_id):
    if not INTEGRATION_TEST_KEY:
        raise Exception("Integration tests require key.")
    monkeypatch.setenv("PROJECT_KEY", INTEGRATION_TEST_KEY)

    class _Basic(DetaModel):
        __db_name__ = "basic" + unique_test_id
        name: str

    yield _Basic
    for item in _Basic.get_all():
        item.delete()
    monkeypatch.setenv("PROJECT_KEY", "123_123")


@pytest.fixture
def item1(IntegrationBasic):
    item = IntegrationBasic(name=Faker().name())
    item.save()
    return item


@pytest.fixture
def gen_basic_items(IntegrationBasic):
    def _gen_items(count, save=True):
        fake = Faker()
        items = []
        for _ in range(count):
            item = IntegrationBasic(name=fake.name())
            if save:
                item.save()
            items.append(item)
        return items

    return _gen_items


@pytest.fixture
def IntegrationMoreAttrs(monkeypatch, unique_test_id):
    if not INTEGRATION_TEST_KEY:
        raise Exception("Integration tests require key.")
    monkeypatch.setenv("PROJECT_KEY", INTEGRATION_TEST_KEY)

    class _MoreAttrs(DetaModel):
        __db_name__ = "moreattrs" + unique_test_id
        name: str
        group: int
        dt: datetime.datetime

    yield _MoreAttrs
    for item in _MoreAttrs.get_all():
        item.delete()
    monkeypatch.setenv("PROJECT_KEY", "123_123")


@pytest.fixture
def gen_more_attrs_items(IntegrationMoreAttrs):
    def _gen_items(count, group=1, dt=None, save=True):
        if not dt:
            dt = datetime.datetime.now()
        fake = Faker()
        items = []
        for _ in range(count):
            item = IntegrationMoreAttrs(name=fake.name(), group=group, dt=dt)
            if save:
                item.save()
            items.append(item)
        return items

    return _gen_items


@pytest.mark.skipif(
    not os.getenv("INTEGRATION_TEST_KEY", None),
    reason="Integration Tests require project to test against.",
)
def test_create_basic(monkeypatch, IntegrationBasic):
    name = Faker().name()

    item = IntegrationBasic(name=name)
    item.save()

    assert item.key is not None
    assert item.name == name


@pytest.mark.skipif(
    not os.getenv("INTEGRATION_TEST_KEY", None),
    reason="Integration Tests require project to test against.",
)
def test_get_basic(item1, IntegrationBasic):
    # get the item by the key of the fixture item
    res_item = IntegrationBasic.get(item1.key)
    # check that they are identical
    for k, v in item1._serialize().items():
        assert getattr(res_item, k) == v


@pytest.mark.skipif(
    not os.getenv("INTEGRATION_TEST_KEY", None),
    reason="Integration Tests require project to test against.",
)
def test_get_all(gen_basic_items, IntegrationBasic):
    _ = gen_basic_items(10)
    records = IntegrationBasic.get_all()
    assert len(records) == 10


@pytest.mark.skipif(
    not os.getenv("INTEGRATION_TEST_KEY", None),
    reason="Integration Tests require project to test against.",
)
def test_query(gen_more_attrs_items, IntegrationMoreAttrs):
    group1 = gen_more_attrs_items(10, 1)
    group2 = gen_more_attrs_items(5, 2)

    found = IntegrationMoreAttrs.query(IntegrationMoreAttrs.group == 1)

    assert len(found) == len(group1)

    found_keys = list(reduce_items_to_keys(found))
    for key in reduce_items_to_keys(group1):
        assert key in found_keys

    for key in reduce_items_to_keys(group2):
        assert key not in found_keys


@pytest.mark.skipif(
    not os.getenv("INTEGRATION_TEST_KEY", None),
    reason="Integration Tests require project to test against.",
)
def test_datetime_query(gen_more_attrs_items, IntegrationMoreAttrs):
    group1 = gen_more_attrs_items(10, 1)
    group2 = gen_more_attrs_items(
        5, 2, datetime.datetime.now() - datetime.timedelta(days=5)
    )

    found = IntegrationMoreAttrs.query(
        IntegrationMoreAttrs.dt < datetime.datetime.now() - datetime.timedelta(hours=36)
    )

    assert len(found) == len(group2)

    found_keys = list(reduce_items_to_keys(found))
    for key in reduce_items_to_keys(group2):
        assert key in found_keys

    for key in reduce_items_to_keys(group1):
        assert key not in found_keys


@pytest.mark.skipif(
    not os.getenv("INTEGRATION_TEST_KEY", None),
    reason="Integration Tests require project to test against.",
)
def test_delete_key(IntegrationBasic):
    item = IntegrationBasic(name="test")
    item.save()
    IntegrationBasic.delete_key(item.key)

    with pytest.raises(ItemNotFound):
        IntegrationBasic.get(item.key)


@pytest.mark.skipif(
    not os.getenv("INTEGRATION_TEST_KEY", None),
    reason="Integration Tests require project to test against.",
)
def test_put_many(IntegrationBasic, gen_basic_items):
    items = gen_basic_items(10, save=False)
    results = IntegrationBasic.put_many(items)

    assert len(results) == 10
    for item in results:
        assert item.key is not None


@pytest.mark.skipif(
    not os.getenv("INTEGRATION_TEST_KEY", None),
    reason="Integration Tests require project to test against.",
)
def test_update(IntegrationBasic):
    name1 = Faker().name()
    item = IntegrationBasic(name=name1)
    item.save()

    assert item.key is not None
    assert item.name == name1

    name2 = Faker().name()
    item.name = name2
    item.save()

    assert item.name == name2

    assert IntegrationBasic.get(item.key).name == name2


@pytest.mark.skipif(
    not os.getenv("INTEGRATION_TEST_KEY", None),
    reason="Integration Tests require project to test against.",
)
def test_delete(IntegrationBasic, gen_basic_items):
    item = gen_basic_items(1)[0]
    old_key = item.key
    item.delete()
    assert item.key is None

    with pytest.raises(ItemNotFound):
        IntegrationBasic.get(old_key)
