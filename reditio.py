from redis import Redis
from pydantic import BaseModel, StrBytes
from typing import Optional, Type, List, Dict, TypeVar, Generic

T = TypeVar('T', BaseModel, str)


class RedisContainer(Generic[T]):
    def __init__(
        self,
        key: str,
        model: Type[T] = str,
        redis: Redis = Redis(),
    ):
        self.key = key
        self.model = model
        self.red = redis

    def _parse_key(self, value: StrBytes) -> str:
        if isinstance(value, bytes):
            return value.decode()
        return value

    def _parse_value(self, value: StrBytes) -> Optional[T]:
        if not value:
            return None
        if issubclass(self.model, BaseModel):
            return self.model.parse_raw(value)
        if not issubclass(self.model, (str, bytes)):
            raise RuntimeError(f'Invalid model type: {self.model}')
        return value

    def _serialize_value(self, value: T) -> StrBytes:
        if self.model is not str:
            return value.json()
        return value


class RKey(Generic[T], RedisContainer[T]):
    def get(self) -> Optional[T]:
        return self._parse_value(self.red.get(self.key))

    def set(self, value: T):
        serialized_value = self._serialize_value(value)
        self.red.set(self.key, serialized_value)


class RList(Generic[T], RedisContainer[T]):
    def getrange(self, start: int, end: int) -> List[T]:
        return [
            self._parse_value(v) for v in self.red.lrange(self.key, start, end)
        ]

    def append(self, value: T):
        serialized_value = self._serialize_value(value)
        self.red.rpush(self.key, serialized_value)

    def bpop(self, timeout=0) -> Optional[T]:
        return self._parse_value(self.red.blpop(self.key, timeout))


class RSet(Generic[T], RedisContainer[T]):
    def members(self) -> list[T]:
        return [self._parse_value(v) for v in self.red.smembers(self.key)]

    def add(self, value: T):
        serialized_value = self._serialize_value(value)
        self.red.sadd(self.key, serialized_value)


class RSortedSet(Generic[T], RedisContainer[T]):
    def getrange(self, start: int, end: int) -> List[T]:
        return [
            self._parse_value(v) for v in self.red.zrange(self.key, start, end)
        ]

    def add(self, value: T, score: float):
        serialized_value = self._serialize_value(value)
        self.red.zadd(self.key, {serialized_value: score})


class RHash(Generic[T], RedisContainer[T]):
    def getall(self) -> Dict[StrBytes, T]:
        raw_dict = self.red.hgetall(self.key)
        return {
            self._parse_key(k): self._parse_value(v)
            for k, v in raw_dict.items()
        }

    def get(self, field: StrBytes) -> Optional[T]:
        return self._parse_value(self.red.hget(self.key, field))

    def set(self, field: StrBytes, value: T):
        serialized_value = self._serialize_value(value)
        self.red.hset(self.key, field, serialized_value)


class Reditio:
    def __init__(self, red: Redis = Redis()):
        self.red = red

    def key(self, key: str, model: Type[T] = str) -> RKey[T]:
        return RKey[T](key, model, self.red)

    def list(self, key: str, model: Type[T] = str) -> RList[T]:
        return RList[T](key, model, self.red)

    def set(self, key: str, model: Type[T] = str) -> RSet[T]:
        return RSet[T](key, model, self.red)

    def sorted_set(self, key: str, model: Type[T] = str) -> RSortedSet[T]:
        return RSortedSet[T](key, model, self.red)

    def hash(self, key: str, model: Type[T] = str) -> RHash[T]:
        return RHash[T](key, model, self.red)
