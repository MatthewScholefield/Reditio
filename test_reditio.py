import pytest
import redis
from reditio import Reditio
from pydantic import BaseModel
from typing import Dict, List, Set


class Person(BaseModel):
    name: str
    age: int


@pytest.fixture(scope='module')
def reditio():
    reditio = Reditio(redis.Redis(db=13))
    yield reditio
    reditio.red.flushall()


def test_rkey(reditio: Reditio):
    key = 'test_key'
    rkey = reditio.key(key, Person)
    rkey.set(Person(name='John', age=30))
    person = rkey.get()
    assert person.name == 'John'
    assert person.age == 30


def test_rlist(reditio: Reditio):
    key = 'test_list'
    rlist = reditio.list(key, Person)
    rlist.append(Person(name='John', age=30))
    rlist.append(Person(name='Jane', age=28))
    people: List[Person] = rlist.getrange(0, -1)
    assert len(people) == 2
    assert people[0].name == 'John'
    assert people[0].age == 30
    assert people[1].name == 'Jane'
    assert people[1].age == 28


def test_rset(reditio: Reditio):
    key = 'test_set'
    rset = reditio.set(key, Person)
    rset.add(Person(name='John', age=30))
    rset.add(Person(name='Jane', age=28))
    people: Set[Person] = rset.members()
    assert len(people) == 2
    assert any(p.name == 'John' and p.age == 30 for p in people)
    assert any(p.name == 'Jane' and p.age == 28 for p in people)


def test_rsortedset(reditio: Reditio):
    key = 'test_sorted_set'
    rsortedset = reditio.sorted_set(key, Person)
    rsortedset.add(Person(name='John', age=30), 1)
    rsortedset.add(Person(name='Jane', age=28), 2)
    people: List[Person] = rsortedset.getrange(0, -1)
    assert len(people) == 2
    assert people[0].name == 'John'
    assert people[0].age == 30
    assert people[1].name == 'Jane'
    assert people[1].age == 28


def test_rhash(reditio: Reditio):
    key = 'test_hash'
    rhash = reditio.hash(key, Person)
    rhash.set('john', Person(name='John', age=30))
    rhash.set('jane', Person(name='Jane', age=28))
    people: Dict[str, Person] = rhash.getall()
    assert len(people) == 2
    assert people['john'].name == 'John'
    assert people['john'].age == 30
    assert people['jane'].name == 'Jane'
    assert people['jane'].age == 28
    assert rhash.get('john') == people['john']
