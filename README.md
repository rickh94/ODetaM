# ODetaM

A simple ODM (Object Document Mapper) for [Deta Base](https://deta.sh) base on 
[pydantic](https://github.com/samuelcolvin/pydantic/).


## Usage

Create pydantic models as normal, but inherit from `DetaModel` instead of pydantic 
BaseModel. You will need to set the environment variable `PROJECT_KEY` to your
Deta project key so that databases can be accessed/created. This is a secret key, 
so handle it appropriately (hence the environment variable). Intended for use with
FastAPI, but the Deta API is not asynchronous, so any framework could potentially
be used.

Bases will be automatically created based on model names (changed from Pascal/Camel 
case to snake case). A 'key' field will be automatically added to any model. You can
supply the key on creation, or Deta will generate one automatically and it will be
added to the object when it is saved.

## Example

```
from odetam import DetaModel

class Captain(DetaModel):
    name: str
    joined: datetime.date
    ships: List[str]

# create
kirk = Captain(
    name="James T. Kirk", 
    joined=datetime.date(2252, 01, 01), 
    ships=["Enterprise"],
)

sisko = Captain(
    name="Benjamin Sisko",
    joined=datetime.date(2350, 01, 01),
    ships=["Deep Space 9", "Defiant"],
)

# initial save, key is now set
kirk.save()

# update the object
kirk.ships.append("Enterprise-A")

# save again, this will be an update
kirk.save()

sisko.save()

Captain.get_all()
# [
#     Captain(
#         name="James T. Kirk", 
#         joined=datetime.date(2252, 01, 01), 
#         ships=["Enterprise"],
#         key="key1",
#     ),
#     Captain(
#         name="Benjamin Sisko",
#         joined=datetime.date(2350, 01, 01), 
#         ships=["Deep Space 9", "Defiant"],
#         key="key2",
#     ),
# ]

Captain.get("key1")
# Captain(
#     name="James T. Kirk", 
#     joined=datetime.date(2252, 01, 01), 
#     ships=["Enterprise"],
#     key="key1",
# )

Captain.query(Captain.name == "James T. Kirk")
# Captain(
#     name="James T. Kirk", 
#     joined=datetime.date(2252, 01, 01), 
#     ships=["Enterprise"],
#     key="key1",
# )

Captain.query(Captain.ships.contains("Defiant"))
# Captain(
#     name="Benjamin Sisko",
#     joined=datetime.date(2350, 01, 01),
#     ships=["Deep Space 9", "Defiant"],
# )

Captain.query(Captain.name.prefix("Ben"))
# Captain(
#     name="Benjamin Sisko",
#     joined=datetime.date(2350, 01, 01),
#     ships=["Deep Space 9", "Defiant"],
# )

kirk.delete()
Captain.delete_key("key2")

Captain.get_all()
# []

# you can also save several at once for better speed
Captain.put_many([kirk, sisko])

```

## Save
Models have the `.save()` method which will always behave as an upsert, updating a record
if it has a key, otherwise creating it and setting a key. Deta has pure insert 
behavior, but it's less performant. If you need it, please open a pull request.

## Querying
All basic comparison operators are implemented to map to their equivalents as 
`(Model.field >= comparison_value)`. There is also a `.contains()` method for
strings and lists of strings, as well as a `.prefix()` method for strings. You can also 
use `&`  as AND and `|` as OR. ORs cannot be nested within ands, use a list of options
as comparison instead. You can use as many ORs as you want, as long as they execute
after the ANDs in the order of operations. This is due to how the Deta Base api 
works.

## Deta Base
Direct access to the base is available in the dunder attribute `__db__`, though
the point is to avoid that.

## Exceptions
- `DetaError`: Base exception when anything goes wrong.
- `ItemNotFound`: Fairly self-explanatory...
- `NoProjectKey`: `PROJECT_KEY` env var has not been set correctly. See Deta 
documentation.
- `InvalidDetaQuery`: Something is wrong with queries. Make sure you aren't using
queries with unsupported types