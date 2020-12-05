from typing import List

import ujson
from pydantic import BaseModel

from odetam.exceptions import InvalidDetaQuery
from odetam.query import DetaQuery


class DetaField:
    def __init__(self, field):
        self.field = field

    def query_expression(self, operator, data):
        if isinstance(data, BaseModel):
            data = ujson.loads(data.json())
        return DetaQuery(condition=f"{self.field.name}?{operator}", value=data)

    def __eq__(self, other):
        return self.query_expression("eq", other)

    def __ne__(self, other):
        return self.query_expression("ne", other)

    def __lt__(self, other):
        return self.query_expression("lt", other)

    def __gt__(self, other):
        return self.query_expression("gt", other)

    def __lte__(self, other):
        return self.query_expression("lte", other)

    def __gte__(self, other):
        return self.query_expression("gte", other)

    def prefix(self, other):
        if not isinstance(other, str) or self.field.type_ != str:
            raise InvalidDetaQuery("Prefix is only valid for string types")
        return self.query_expression("pfx", other)

    def range(self, range_lst: List[int]):
        if not isinstance(range_lst, list) or len(range_lst) != 2:
            raise InvalidDetaQuery("Range List must be two integers")
        return DetaQuery(condition=f"{self.field.name}?r", value=range_lst)

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
