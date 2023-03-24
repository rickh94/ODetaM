import datetime
import re
from typing import Any, Callable, Container, Dict, List, Optional, Union

import pydantic
import ujson
from deta import Base, Deta
from deta.base import FetchResponse, _Base
from pydantic import BaseModel, Field, ValidationError
from typing_extensions import Self

from odetam.exceptions import DetaError, InvalidKey, ItemNotFound
from odetam.field import DetaField
from odetam.query import DetaQuery, DetaQueryList, DetaQueryStatement

DETA_BASIC_TYPES = [Dict[str, Any], List[Any], str, int, float, bool]
DETA_OPTIONAL_TYPES = [Optional[type_] for type_ in DETA_BASIC_TYPES]
DETA_BASIC_LIST_TYPES = [
    List[type_] for type_ in DETA_BASIC_TYPES + DETA_OPTIONAL_TYPES
]
DETA_TYPES = DETA_BASIC_TYPES + DETA_OPTIONAL_TYPES + DETA_BASIC_LIST_TYPES


def handle_db_property(
    cls: "BaseDetaModel", base_class: Callable[[str], _Base]
) -> _Base:
    if cls._db:
        return cls._db

    cls._db = base_class(cls.__db_name__)
    return cls._db


class DetaModelMetaClass(pydantic.main.ModelMetaclass):
    def __new__(mcs, name, bases, dct):
        cls = super().__new__(mcs, name, bases, dct)
        if getattr(cls.Config, "table_name", None) is not None:
            cls.__db_name__ = cls.Config.table_name
        else:
            cls.__db_name__ = re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()
        cls._db = None

        for name, field in cls.__fields__.items():
            setattr(cls, name, DetaField(field=field))
        return cls

    @property
    def __db__(cls):
        if getattr(cls.Config, "deta_key", None) is not None:
            deta = Deta(cls.Config.deta_key)
            return handle_db_property(cls, deta.Base)

        return handle_db_property(cls, Base)


class BaseDetaModel(BaseModel):
    __db__ = Optional[_Base]

    key: Optional[str] = Field(
        default=None, title="Key", description="Primary key in the database"
    )

    def _serialize(self, exclude: Optional[Container[str]] = None) -> Dict[str, Any]:
        if not exclude:
            exclude = []

        as_dict: Dict[str, Any] = {}
        for field_name, field in self.__class__.__fields__.items():
            if field_name in exclude:
                continue
            elif field_name == "key" and not self.key:
                continue
            elif getattr(self, field_name, None) is None:
                as_dict[field_name] = None
            elif field.type_ in DETA_TYPES:
                as_dict[field_name] = getattr(self, field_name)
            elif field.type_ == datetime.datetime:
                as_dict[field_name] = getattr(self, field_name).timestamp()
            elif field.type_ == datetime.date:
                as_dict[field_name] = int(getattr(self, field_name).strftime("%Y%m%d"))
            elif field.type_ == datetime.time:
                as_dict[field_name] = int(
                    getattr(self, field_name).strftime("%H%M%S%f")
                )
            else:
                as_dict[field_name] = ujson.loads(self.json(include={field_name}))[
                    field_name
                ]

        return as_dict

    @classmethod
    def _deserialize(cls, data: Dict[str, Any]) -> Self:
        as_dict: Dict[str, Any] = {}
        for field_name, field in cls.__fields__.items():
            if data.get(field_name) is None:
                as_dict[field_name] = None
            elif field.type_ in DETA_TYPES:
                as_dict[field_name] = data[field_name]
            elif field.type_ == datetime.datetime:
                as_dict[field_name] = datetime.datetime.fromtimestamp(data[field_name])
            elif field.type_ == datetime.date:
                as_dict[field_name] = datetime.datetime.strptime(
                    str(data[field_name]), "%Y%m%d"
                ).date()
            elif field.type_ == datetime.time:
                as_dict[field_name] = datetime.datetime.strptime(
                    str(data[field_name]), "%H%M%S%f"
                ).time()
            else:
                value = data.get(field_name)
                # value is guaranteed to not be none above
                try:
                    as_dict[field_name] = ujson.loads(value)
                except (TypeError, ValueError):
                    as_dict[field_name] = value

        return cls.parse_obj(as_dict)

    @classmethod
    def _return_item_or_raise(cls, item: Optional[Dict[str, Any]]) -> Self:
        if item is None or item.get("key") == "None":
            raise ItemNotFound("Could not find item matching that key")
        try:
            return cls._deserialize(item)
        except ValidationError:
            raise ItemNotFound("Could not find item matching that key")


class DetaModel(BaseDetaModel, metaclass=DetaModelMetaClass):
    @classmethod
    def get(cls, key: str) -> Self:
        """
        Get a single instance
        :param key: Deta database key
        :return: object found in database serialized into its pydantic object

        :raises ItemNotFound: No matching item was found
        """
        if key is None:
            raise InvalidKey("key cannot be None")

        item: Dict[str, Any] = cls.__db__.get(key)
        return cls._return_item_or_raise(item)

    @classmethod
    def get_or_none(cls, key: str) -> Optional[Self]:
        """Try to get item by key or return None if item not found"""
        try:
            return cls.get(key)
        except ItemNotFound:
            return None

    @classmethod
    def get_all(cls) -> List[Self]:
        """Get all the records from the database"""
        response: FetchResponse = cls.__db__.fetch()
        records: List[Dict[str, Any]] = response.items
        while response.last:
            response = cls.__db__.fetch(last=response.last)
            records += response.items

        return [cls._deserialize(record) for record in records]

    @classmethod
    def query(
        cls, query_statement: Union[DetaQuery, DetaQueryStatement, DetaQueryList]
    ) -> List[Self]:
        """Get items from database based on the query."""
        response: FetchResponse = cls.__db__.fetch(query_statement.as_query())
        records: List[Dict[str, Any]] = response.items
        while response.last:
            response = cls.__db__.fetch(query_statement.as_query(), last=response.last)
            records += response.items

        return [cls._deserialize(item) for item in records]

    @classmethod
    def delete_key(cls, key: str):
        """Delete an item based on the key"""
        cls.__db__.delete(key)

    @classmethod
    def put_many(cls, items: List[Self]) -> List[Self]:
        """Put multiple instances at once

        :param items: List of pydantic objects to put in the database
        :returns: List of items successfully added, serialized with pydantic
        """
        records = []
        processed: List[Dict[str, Any]] = []
        for item in items:
            exclude: set[str] = set()
            if item.key is None:
                exclude = {"key"}
            # noinspection PyProtectedMember
            records.append(item._serialize(exclude=exclude))
            if len(records) == 25:
                result = cls.__db__.put_many(records)
                processed.extend(result["processed"]["items"])
                records = []
        if records:
            result = cls.__db__.put_many(records)
            processed.extend(result["processed"]["items"])

        return [cls._deserialize(record) for record in processed]

    @classmethod
    def _db_put(cls, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return cls.__db__.put(data)  # type: ignore

    def save(self):
        """Saves the record to the database. Behaves as upsert, will create
        if not present. Database key will then be set on the object."""
        # exclude = set()
        # if self.key is None:
        #     exclude.add("key")
        # # this is dumb, but it ensures everything is in a json-serializable form
        # data = ujson.loads(self.json(exclude=exclude))
        saved = self._db_put(self._serialize())
        self.key = saved["key"]

    def delete(self):
        """Delete the open object from the database. The object will still exist in
        python, but will be deleted from the database and the key attribute will be
        set to None."""
        if not self.key:
            raise DetaError("Item does not have key for deletion")
        self.delete_key(self.key)
        self.key = None
