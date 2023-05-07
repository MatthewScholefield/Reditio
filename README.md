# Reditio

*A simple typed Python interface to Redis*

Reditio is a Python library that provides a high-level, typed interface to Redis using Pydantic.

```python
reditio = Reditio()

users = reditio.hash('users', User)
users.set('123', User(name='John'))
users.set('124', User(name='Mary'))

names = [user.name for user in users.getall().values()]
print('Users:', ', '.join(names))
```

## Installation

You can install Reditio using pip:

```
pip install reditio
```

## Basic Usage

Here's a tiny example of how you can use Reditio to set and get a key in Redis:

```python
from reditio import Reditio
from pydantic import BaseModel

class MyData(BaseModel):
    name: str
    age: int

r = Reditio()
key = r.key('my-key', model=MyData)
key.set(MyData(name='Alice', age=30))
data = key.get()
print(data) # Output: MyData(name='Alice', age=30)
```

In this example, we define a Pydantic model `MyData` to represent our data. We then create an instance of `Reditio` and a `key` object for the Redis key named `my-key`, using `MyData` as the model. We set the value of the key to an instance of `MyData`, and then retrieve it back using the `get` method. The retrieved value is automatically parsed and validated as an instance of `MyData`.

Reditio provides similar interfaces for other Redis data structures. You can create a `list`, `set`, `sorted_set`, or `hash` object using the `list`, `set`, `sorted_set`, or `hash` methods of `Reditio`, respectively, and perform operations on them using the methods provided by each object.

## Related Projects

 - [`walrus`](https://github.com/coleifer/walrus): Similar idea, but with no Pydantic support or support for basic key value objects
