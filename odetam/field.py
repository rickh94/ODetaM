import datetime
from typing import Any, Dict, List, Union

import ujson
from pydantic import BaseModel
from pydantic.fields import ModelField

from odetam.exceptions import InvalidDetaQuery
from odetam.query import DetaQuery

NON_STR_TYPES = [
    Dict[str, Any],
    int,
    float,
    bool,
    List[Dict[str, Any]],
    List[float],
    List[int],
    List[float],
    List[bool],
    datetime.datetime,
    datetime.date,
    datetime.time,
]


def _handle_datetimes(
    possible_datetime: Union[datetime.datetime, datetime.date, datetime.time, Any]
) -> Any:
    if isinstance(possible_datetime, datetime.datetime):
        return possible_datetime.timestamp()
    elif isinstance(possible_datetime, datetime.date):
        return int(possible_datetime.strftime("%Y%m%d"))
    elif isinstance(possible_datetime, datetime.time):
        return int(possible_datetime.strftime("%H%M%S%f"))
    return possible_datetime


class DetaField:
    def __init__(self, field: ModelField):
        self.field = field

    def _check_type(self, other: Any):
        if not isinstance(other, self.field.type_):
            raise TypeError("Cannot compare different types")

    def _query_expression(self, operator: str, data: Any) -> DetaQuery:
        if isinstance(data, BaseModel):
            data = ujson.loads(data.json())
        return DetaQuery(condition=f"{self.field.name}?{operator}", value=data)

    def __eq__(self, other: "DetaField") -> DetaQuery:  # type: ignore
        self._check_type(other)
        return DetaQuery(condition=self.field.name, value=_handle_datetimes(other))

    def __ne__(self, other: "DetaField") -> DetaQuery:  # type: ignore
        self._check_type(other)
        return self._query_expression("ne", _handle_datetimes(other))

    def __lt__(self, other: "DetaField") -> DetaQuery:
        self._check_type(other)
        return self._query_expression("lt", _handle_datetimes(other))

    def __gt__(self, other: "DetaField") -> DetaQuery:
        self._check_type(other)
        return self._query_expression("gt", _handle_datetimes(other))

    def __le__(self, other: "DetaField") -> DetaQuery:
        self._check_type(other)
        return self._query_expression("lte", _handle_datetimes(other))

    def __ge__(self, other: "DetaField") -> DetaQuery:
        self._check_type(other)
        return self._query_expression("gte", _handle_datetimes(other))

    def prefix(self, other: "DetaField") -> DetaQuery:
        if not isinstance(other, str) or self.field.type_ != str:
            raise InvalidDetaQuery("Prefix is only valid for string types")
        return self._query_expression("pfx", other)

    def range(self, lower: Union[int, float], upper: Union[int, float]) -> DetaQuery:
        self._check_type(lower)
        self._check_type(upper)
        if self.field.type_ not in (
            int,
            float,
            datetime.date,
            datetime.time,
            datetime.datetime,
        ):
            raise TypeError("Range is only valid for number types")
        if self.field.type_ in (datetime.date, datetime.time, datetime.datetime):
            lower = _handle_datetimes(lower)
            upper = _handle_datetimes(upper)
        if upper <= lower:
            raise InvalidDetaQuery("Lower must be less than upper")
        return DetaQuery(condition=f"{self.field.name}?r", value=[lower, upper])

    def contains(self, other: Any) -> DetaQuery:
        if not isinstance(other, str) or self.field.type_ in NON_STR_TYPES:
            raise InvalidDetaQuery(
                "Contains is only valid for strings or lists of strings"
            )
        return DetaQuery(condition=f"{self.field.name}?contains", value=other)

    def not_contains(self, other: Any) -> DetaQuery:
        if not isinstance(other, str) or self.field.type_ in NON_STR_TYPES:
            raise InvalidDetaQuery(
                "Not contains is only valid for strings or lists of strings"
            )
        return DetaQuery(condition=f"{self.field.name}?not_contains", value=other)
