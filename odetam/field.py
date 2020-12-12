from typing import List

import ujson
from pydantic import BaseModel

from odetam.exceptions import InvalidDetaQuery
from odetam.query import DetaQuery


class DetaField:
    def __init__(self, field):
        self.field = field

    def _query_expression(self, operator, data):
        if isinstance(data, BaseModel):
            data = ujson.loads(data.json())
        return DetaQuery(condition=f"{self.field.name}?{operator}", value=data)

    def __eq__(self, other):
        return DetaQuery(condition=self.field.name, value=other)

    def __ne__(self, other):
        return self._query_expression("ne", other)

    def __lt__(self, other):
        return self._query_expression("lt", other)

    def __gt__(self, other):
        return self._query_expression("gt", other)

    def __le__(self, other):
        return self._query_expression("lte", other)

    def __ge__(self, other):
        return self._query_expression("gte", other)

    def prefix(self, other):
        if not isinstance(other, str) or self.field.type_ != str:
            raise InvalidDetaQuery("Prefix is only valid for string types")
        return self._query_expression("pfx", other)

    def range(self, lower, upper):
        if self.field.type_ not in (int, float):
            raise InvalidDetaQuery("Range is only valid for number types")
        if not isinstance(lower, (int, float)):
            raise InvalidDetaQuery("Lower must be a number")
        if not isinstance(upper, (int, float)):
            raise InvalidDetaQuery("Upper must be a number")
        if upper <= lower:
            raise InvalidDetaQuery("Lower must be less than upper")
        return DetaQuery(condition=f"{self.field.name}?r", value=[lower, upper])

    def contains(self, other: str):
        if not isinstance(other, str) or self.field.type_ not in [str, List[str]]:
            raise InvalidDetaQuery(
                "Contains is only valid for strings or lists of strings"
            )
        return DetaQuery(condition=f"{self.field.name}?contains", value=other)

    def not_contains(self, other: str):
        if not isinstance(other, str) or self.field.type_ not in [str, List[str]]:
            raise InvalidDetaQuery(
                "Not contains is only valid for strings or lists of strings"
            )
        return DetaQuery(condition=f"{self.field.name}?not_contains", value=other)
