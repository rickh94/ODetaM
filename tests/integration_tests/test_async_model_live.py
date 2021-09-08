import datetime
import os

import pytest
from faker import Faker

from odetam.async_model import AsyncDetaModel
from odetam.exceptions import ItemNotFound

INTEGRATION_TEST_KEY = os.getenv("INTEGRATION_TEST_KEY")


def reduce_items_to_keys(items):
    for item in items:
        yield item.key


@pytest.fixture
@pytest.mark.asyncio
async def IntegrationAsyncBasic(monkeypatch, unique_test_id):
    if not INTEGRATION_TEST_KEY:
        raise Exception("Integration tests require key.")
    monkeypatch.setenv("PROJECT_KEY", INTEGRATION_TEST_KEY)

    class _AsyncBasic(AsyncDetaModel):
        name: str

        class Config:
            table_name = "asyncbasic." + unique_test_id

    yield _AsyncBasic
    for item in await _AsyncBasic.get_all():
        await item.delete()
    monkeypatch.setenv("PROJECT_KEY", "123_123")


@pytest.fixture
@pytest.mark.asyncio
async def item1(IntegrationAsyncBasic):
    item = IntegrationAsyncBasic(name=Faker().name())
    await item.save()
    return item


@pytest.fixture
@pytest.mark.asyncio
async def gen_async_basic_items(IntegrationAsyncBasic):
    async def _gen_items(count, save=True):
        fake = Faker()
        items = []
        for _ in range(count):
            item = IntegrationAsyncBasic(name=fake.name())
            if save:
                await item.save()
            items.append(item)
        return items

    return _gen_items


@pytest.fixture
@pytest.mark.asyncio
async def IntegrationAsyncMoreAttrs(monkeypatch, unique_test_id):
    if not INTEGRATION_TEST_KEY:
        raise Exception("Integration tests require key.")
    monkeypatch.setenv("PROJECT_KEY", INTEGRATION_TEST_KEY)

    class _AsyncMoreAttrs(AsyncDetaModel):
        name: str
        group: int
        dt: datetime.datetime

        class Config:
            table_name = "asyncmoreattrs." + unique_test_id

    yield _AsyncMoreAttrs
    for item in await _AsyncMoreAttrs.get_all():
        await item.delete()
    monkeypatch.setenv("PROJECT_KEY", "123_123")


@pytest.fixture
def gen_async_more_attrs_items(IntegrationAsyncMoreAttrs):
    async def _gen_items(count, group=1, dt=None, save=True):
        if not dt:
            dt = datetime.datetime.now()
        fake = Faker()
        items = []
        for _ in range(count):
            item = IntegrationAsyncMoreAttrs(name=fake.name(), group=group, dt=dt)
            if save:
                await item.save()
            items.append(item)
        return items

    return _gen_items


@pytest.mark.skipif(
    not os.getenv("INTEGRATION_TEST_KEY", None),
    reason="Integration Tests require project to test against.",
)
@pytest.mark.asyncio
async def test_create_basic(monkeypatch, IntegrationAsyncBasic):
    name = Faker().name()

    item = IntegrationAsyncBasic(name=name)
    await item.save()

    assert item.key is not None
    assert item.name == name


@pytest.mark.skipif(
    not os.getenv("INTEGRATION_TEST_KEY", None),
    reason="Integration Tests require project to test against.",
)
@pytest.mark.asyncio
async def test_async_get_basic(item1, IntegrationAsyncBasic):
    # get the item by the key of the fixture item
    res_item = await IntegrationAsyncBasic.get(item1.key)
    # check that they are identical
    for k, v in item1._serialize().items():
        assert getattr(res_item, k) == v


@pytest.mark.skipif(
    not os.getenv("INTEGRATION_TEST_KEY", None),
    reason="Integration Tests require project to test against.",
)
@pytest.mark.asyncio
async def test_async_get_all(gen_async_basic_items, IntegrationAsyncBasic):
    _ = await gen_async_basic_items(10)
    records = await IntegrationAsyncBasic.get_all()
    assert len(records) == 10


@pytest.mark.skipif(
    not os.getenv("INTEGRATION_TEST_KEY", None),
    reason="Integration Tests require project to test against.",
)
@pytest.mark.asyncio
async def test_async_query(gen_async_more_attrs_items, IntegrationAsyncMoreAttrs):
    group1 = await gen_async_more_attrs_items(10, 1)
    group2 = await gen_async_more_attrs_items(5, 2)

    found = await IntegrationAsyncMoreAttrs.query(IntegrationAsyncMoreAttrs.group == 1)

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
@pytest.mark.asyncio
async def test_async_datetime_query(
    gen_async_more_attrs_items, IntegrationAsyncMoreAttrs
):
    group1 = await gen_async_more_attrs_items(10, 1)
    group2 = await gen_async_more_attrs_items(
        5, 2, datetime.datetime.now() - datetime.timedelta(days=5)
    )

    found = await IntegrationAsyncMoreAttrs.query(
        IntegrationAsyncMoreAttrs.dt
        < datetime.datetime.now() - datetime.timedelta(hours=36)
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
@pytest.mark.asyncio
async def test_async_delete_key(IntegrationAsyncBasic):
    item = IntegrationAsyncBasic(name="test")
    old_key = item.key
    await item.save()
    await IntegrationAsyncBasic.delete_key(item.key)

    with pytest.raises(ItemNotFound):
        await IntegrationAsyncBasic.get(old_key)


@pytest.mark.skipif(
    not os.getenv("INTEGRATION_TEST_KEY", None),
    reason="Integration Tests require project to test against.",
)
@pytest.mark.asyncio
async def test_async_put_many(IntegrationAsyncBasic, gen_async_basic_items):
    items = await gen_async_basic_items(10, save=False)
    results = await IntegrationAsyncBasic.put_many(items)

    assert len(results) == 10
    for item in results:
        assert item.key is not None


@pytest.mark.skipif(
    not os.getenv("INTEGRATION_TEST_KEY", None),
    reason="Integration Tests require project to test against.",
)
@pytest.mark.asyncio
async def test_async_update(IntegrationAsyncBasic):
    name1 = Faker().name()
    item = IntegrationAsyncBasic(name=name1)
    await item.save()

    assert item.key is not None
    assert item.name == name1

    name2 = Faker().name()
    item.name = name2
    await item.save()

    assert item.name == name2

    assert (await IntegrationAsyncBasic.get(item.key)).name == name2


@pytest.mark.skipif(
    not os.getenv("INTEGRATION_TEST_KEY", None),
    reason="Integration Tests require project to test against.",
)
@pytest.mark.asyncio
async def test_async_delete(IntegrationAsyncBasic, gen_async_basic_items):
    item = (await gen_async_basic_items(1))[0]
    old_key = item.key
    await item.delete()
    assert item.key is None

    with pytest.raises(ItemNotFound):
        await IntegrationAsyncBasic.get(old_key)
