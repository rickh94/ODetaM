from typing import Union

from deta import AsyncBase
from deta.base import FetchResponse
from odetam.exceptions import DetaError
from odetam.model import BaseDetaModel, DetaModelMetaClass, handle_db_property
from odetam.query import DetaQueryList, DetaQueryStatement, DetaQuery


class AsyncDetaModelMetaClass(DetaModelMetaClass):
    @property
    def __db__(cls):
        return handle_db_property(cls, AsyncBase)


class AsyncDetaModel(BaseDetaModel, metaclass=AsyncDetaModelMetaClass):
    @classmethod
    async def get(cls, key):
        """
        Get a single instance
        :param key: Deta database key
        :return: object found in database serialized into its pydantic object

        :raises ItemNotFound: No matching item was found
        """
        item = await cls.__db__.get(key)
        return cls._return_item_or_raise(item)

    @classmethod
    async def get_all(cls):
        """Get all the records from the database"""
        response: FetchResponse = await cls.__db__.fetch()
        records = response.items
        while response.last:
            response = await cls.__db__.fetch(last=response.last)
            records += response.items

        return [cls._deserialize(record) for record in records]

    @classmethod
    async def query(
        cls, query_statement: Union[DetaQuery, DetaQueryStatement, DetaQueryList]
    ):
        """Get items from database based on the query."""
        response: FetchResponse = await cls.__db__.fetch(query_statement.as_query())
        records = response.items
        while response.last:
            response = await cls.__db__.fetch(query_statement.as_query(), last=response.last)
            records += response.items

        return [cls._deserialize(item) for item in records]

    @classmethod
    async def delete_key(cls, key):
        """Delete an item based on the key"""
        await cls.__db__.delete(key)

    @classmethod
    async def put_many(cls, items):
        """Put multiple instances at once

        :param items: List of pydantic objects to put in the database
        :returns: List of items successfully added, serialized with pydantic
        """
        records = []
        processed = []
        for item in items:
            exclude = set()
            if item.key is None:
                exclude = {"key"}
            # noinspection PyProtectedMember
            records.append(item._serialize(exclude=exclude))
            if len(records) == 25:
                result = await cls.__db__.put_many(records)
                processed.extend(result["processed"]["items"])
                records = []
        if records:
            result = await cls.__db__.put_many(records)
            processed.extend(result["processed"]["items"])
        return [cls._deserialize(rec) for rec in processed]

    @classmethod
    async def _db_put(cls, data):
        return await cls.__db__.put(data)

    async def save(self):
        """Saves the record to the database. Behaves as upsert, will create
        if not present. Database key will then be set on the object."""
        # exclude = set()
        # if self.key is None:
        #     exclude.add("key")
        # # this is dumb, but it ensures everything is in a json-serializable form
        # data = ujson.loads(self.json(exclude=exclude))
        saved = await self._db_put(self._serialize())
        self.key = saved["key"]

    async def delete(self):
        """Delete the open object from the database. The object will still exist in
        python, but will be deleted from the database and the key attribute will be
        set to None."""
        if not self.key:
            raise DetaError("Item does not have key for deletion.")
        await self.delete_key(self.key)
        self.key = None
