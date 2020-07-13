"""Defines memory transformers"""

from dalio.base import _Transformer


class _Memory(_Transformer):
    """Stored memory

    Attributes:
        _loc: location of memory storage. 
    """

    def __init__(self):
        super().__init__()
        self._loc = None

    def run(self, **kwargs):
        """Check if location is set and return stored data accordingly"""
        raise NotImplementedError()

    def copy(self, *args, **kwargs):
        """Create new instance and set memory locator"""
        ret = type(self)()
        ret.set_memory_location(self._loc)

    def set_input(self, new_input):
        """Check input and set source or location as necessary"""
        raise NotImplementedError()

    def with_input(self, new_input):
        raise NotImplementedError()

    def set_memory_location(self, new_loc):
        """Perform input checks and set new memory location"""
        self._loc = new_loc


class Local(_Memory):
    """Local memory"""
    pass


class Cached(_Memory):
    """Cached memory"""
    pass


class ExternalMem(_Memory):
    """Memory managed by external source"""
    pass
