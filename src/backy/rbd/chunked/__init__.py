from typing import TypeAlias

Hash: TypeAlias = str


class BackendException(IOError):
    pass


class InconsistentHash(BackendException):
    def __init__(self, expected, actual):
        self.expected = expected
        self.actual = actual


from .chunk import Chunk
from .file import File
from .store import Store

__all__ = [
    "Chunk",
    "File",
    "Store",
    "Hash",
    "BackendException",
    "InconsistentHash",
]
