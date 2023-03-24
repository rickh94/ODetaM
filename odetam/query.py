from typing import Any, List, Union, Dict

from typing_extensions import Self

from odetam.exceptions import InvalidDetaQuery


class DetaQuery:
    def __init__(self, condition: str, value: Any):
        self.condition = condition
        self.value = value

    def __and__(
        self, other: Union["DetaQuery", "DetaQueryStatement"]
    ) -> "DetaQueryStatement":
        if isinstance(other, DetaQueryStatement):
            return other & self
        return DetaQueryStatement([self, other])

    def __or__(
        self, other: Union["DetaQuery", "DetaQueryStatement", "DetaQueryList"]
    ) -> "DetaQueryList":
        if isinstance(other, DetaQueryList):
            return other | self
        return DetaQueryList(conditions=[self, other])

    def as_query(self) -> Dict[str, Any]:
        return {self.condition: self.value}


class DetaQueryList:
    def __init__(
        self,
        conditions: List[Union["DetaQuery", "DetaQueryStatement", "DetaQueryList"]],
    ):
        self.conditions = conditions

    def __or__(
        self, other: Union["DetaQuery", "DetaQueryStatement", "DetaQueryList"]
    ) -> Self:
        if isinstance(other, DetaQueryList):
            self.conditions.extend(other.conditions)
        else:
            self.conditions.append(other)
        return self

    def as_query(self) -> List[Any]:
        return [query.as_query() for query in self.conditions]


class DetaQueryStatement:
    def __init__(self, conditions: List[DetaQuery]):
        self.conditions = conditions

    def __and__(self, other: Union["DetaQuery", "DetaQueryStatement"]) -> Self:
        if isinstance(other, DetaQuery):
            self.conditions.append(other)
        if isinstance(other, DetaQueryStatement):
            self.conditions.extend(other.conditions)
        if isinstance(other, DetaQueryList):
            raise InvalidDetaQuery("Incorrect and/or nesting")
        return self

    def __or__(
        self, other: Union["DetaQuery", "DetaQueryStatement", "DetaQueryList"]
    ) -> "DetaQueryList":
        if isinstance(other, DetaQueryList):
            other.conditions.append(self)
            return other
        return DetaQueryList(conditions=[self, other])

    def as_query(self) -> Dict[str, Any]:
        return {query.condition: query.value for query in self.conditions}
