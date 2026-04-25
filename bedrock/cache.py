"""
cache.py
========

"""

# Standard library
import io

# Third-party
import pandas as pd


class CacheItem:
    def __init__(self, value: io.BytesIO, name: str, expires_in: pd.Timedelta):
        self._data: io.BytesIO | None = value
        self._name: str = name
        self._ts: pd.Timestamp = pd.Timestamp.now()
        self._expiration_time: pd.Timedelta = expires_in

    @property
    def data(self) -> io.BytesIO | None:
        now: pd.Timestamp = pd.Timestamp.now()

        if now > (self._ts + self._expiration_time):
            self._data = None
        
        return self._data
    
    @data.setter
    def data(self, value: io.BytesIO) -> None:
        self._data = value
        self._ts = pd.Timestamp.now()

    @property
    def name(self) -> str:
        return self._name


class Cache:
    def __init__(self):
        self._data: dict[str, CacheItem] = {}

    def add(self, item: CacheItem) -> str:
        self._data[item.name] = item

        return item.name
    
    def get(self, name: str) -> CacheItem:
        item: CacheItem  | None = self._data.get(name, None)

        if item is None:
            raise KeyError(f'The CacheItem "{name}" does not exist.')
        
        # Remove if expired
        if item.data is None:
            del self._data[name]
            raise KeyError(f'The CacheItem "{name}" has expired.')
        
        return item
