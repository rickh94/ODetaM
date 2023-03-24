from typing import Any, Dict, List, Optional, Union

from deta import AsyncBase, Deta
from deta.base import FetchResponse
from typing_extensions import Self

from odetam.exceptions import DetaError, InvalidKey, ItemNotFound
from odetam.model import BaseDetaModel, DetaModelMetaClass, handle_db_property
from odetam.query import DetaQuery, DetaQueryList, DetaQueryStatement


class AsyncDetaModelMetaClass(DetaModelMetaClass):
    @property
    def __db__(cls):
        if getattr(cls.Config, "deta_key", None) is not None:
            deta = Deta(cls.Config.deta_key)
            return handle_db_property(cls, deta.AsyncBase)

        return handle_db_property(cls, AsyncBase)


class AsyncDetaModel(BaseDetaModel, metaclass=AsyncDetaModelMetaClass):
    @classmethod
    async def get(cls, key: str) -> Self:
        """
        Get a single instance
        :param key: Deta database key
        :return: object found in database serialized into its pydantic object

        :raises ItemNotFound: No matching item was found
        """
        if key is None:
            raise InvalidKey("key cannot be None")

        item: Dict[str, Any] = await cls.__db__.get(key)
        return cls._return_item_or_raise(item)

    @classmethod
    async def get_or_none(cls, key: str) -> Optional[Self]:
        """Try to get item by key or return None if item not found"""
        try:
            return await cls.get(key)
        except ItemNotFound:
            return None

    @classmethod
    async def get_all(cls) -> List[Self]:
        """Get all the records from the database"""
        response: FetchResponse = await cls.__db__.fetch()
        records: List[Dict[str, Any]] = response.items
        while response.last:
            response = await cls.__db__.fetch(last=response.last)
            records += response.items

        return [cls._deserialize(record) for record in records]

    @classmethod
    async def query(
        cls, query_statement: Union[DetaQuery, DetaQueryStatement, DetaQueryList]
    ) -> List[Self]:
        """Get items from database based on the query."""
        response: FetchResponse = await cls.__db__.fetch(query_statement.as_query())
        records: List[Dict[str, Any]] = response.items
        while response.last:
            response = await cls.__db__.fetch(
                query_statement.as_query(), last=response.last
            )
            records += response.items

        return [cls._deserialize(item) for item in records]

    @classmethod
    async def delete_key(cls, key: str):
        """Delete an item based on the key"""
        await cls.__db__.delete(key)

    @classmethod
    async def put_many(cls, items: List[Self]):
        """Put multiple instances at once

        :param items: List of pydantic objects to put in the database
        :returns: List of items successfully added, serialized with pydantic
        """
        records = []
        processed: List[Dict[str, Any]] = []
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

        return [cls._deserialize(record) for record in processed]

    @classmethod
    async def _db_put(cls, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
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
