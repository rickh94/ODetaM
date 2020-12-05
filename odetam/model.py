import os
import re
from typing import Optional, Union

import pydantic
import ujson
from deta import Deta
from pydantic import Field, BaseModel

from odetam.exceptions import NoProjectKey, DetaError, ItemNotFound
from odetam.field import DetaField
from odetam.query import DetaQuery, DetaQueryStatement, DetaQueryList


class DetaModelMetaClass(pydantic.main.ModelMetaclass):
    def __new__(mcs, name, bases, dct):
        cls = super().__new__(mcs, name, bases, dct)
        cls.__db_name__ = re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()
        try:
            deta = Deta(os.getenv("PROJECT_KEY"))
        except AttributeError:
            raise NoProjectKey(
                "Ensure that the 'PROJECT_KEY' environment variable is set to your "
                "project key"
            )
        cls.__db__ = deta.Base(cls.__db_name__)

        for name, field in cls.__fields__.items():
            setattr(cls, name, DetaField(field=field))
        return cls


class DetaModel(BaseModel, metaclass=DetaModelMetaClass):
    __db__ = None
    key: Optional[str] = Field(
        None, title="Key", description="Primary key in the database"
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.key = self.key or None

    @classmethod
    def get(cls, key):
        """
        Get a single instance
        :param key: Deta database key
        :return: object found in database serialized into its pydantic object

        :raises NotImplemented: Very bad error, probably in this code
        :raises ItemNotFound: No matching item was found
        """
        if cls.__db__ is None:
            raise NotImplemented(
                "Db has not been set. Something went wrong in the inheritance."
            )
        item = cls.__db__.get(key)
        if item is None:
            raise ItemNotFound("Could not find item matching that key")
        return cls.parse_obj(item)

    @classmethod
    def get_all(cls):
        """Get all the records from the database"""
        records = next(cls.__db__.fetch())
        return [cls.parse_obj(record) for record in records]

    @classmethod
    def query(
        cls, query_statement: Union[DetaQuery, DetaQueryStatement, DetaQueryList]
    ):
        """Get items from database based on the query."""
        found = next(cls.__db__.fetch(query_statement.as_query()))
        return [cls.parse_obj(item) for item in found]

    @classmethod
    def delete_key(cls, key):
        """Delete an item based on the key"""
        cls.__db__.delete(key)

    @classmethod
    def put_many(cls, items):
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
            data = ujson.loads(item.json(exclude=exclude))
            records.append(data)
            if len(records) == 25:
                result = cls.__db__.put_many(records)
                processed.extend(result["processed"]["items"])
                records = []
        if records:
            cls.__db__.put_many(records)
        return [cls.parse_obj(rec) for rec in processed]

    def save(self):
        """Saves the record to the database. Behaves as upsert, will create
        if not present. Database key will then be set on the object."""
        exclude = set()
        if self.key is None:
            exclude.add("key")
        # this is dumb, but it ensures everything is in a json-serializable form
        data = ujson.loads(self.json(exclude=exclude))
        saved = self.__db__.put(data)
        self.key = saved["key"]

    def delete(self):
        """Delete the open object from the database. The object will still exist in
        python, but will be deleted from the database and the key attribute will be
        set to None."""
        if not self.key:
            raise DetaError("Item does not have key for deletion")
        self.__db__.delete(self.key)
        self.key = None
