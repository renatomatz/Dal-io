"""Define memory interpreters"""

from copy import copy

from dalio.base import _DataDef

from dalio.interpreter import _Interpreter


class _Memory(_Interpreter):
    """Implement mechanics to store and retrieve input data.

    This is a pseudo-transformer, as it is supposed to behave like on on the
    surface (implementing all needed methods) but not actually performing any
    actual transformation.

    This is used in pipes that heavily reutilize the same external data source
    using the same kwarg requests. Implementations store and retrieive data
    through different methods and locations, and might implement certain
    requirements that must be met by input data in order for it to be stored.

    Attributes:
        _def (_DataDef): Connection-less data definition that checks for
            required characteristics of of input data.
        _engine (any): Memory source. Implementations will often have
            additional attributes to manage this source.
    """

    def __init__(self):
        super().__init__()
        self._def = _DataDef()
        self.clear()

    def load(self, **kwargs):
        """Check if location is set and return stored data accordingly"""
        raise NotImplementedError()

    def save(self, data):
        """Store input data"""
        raise NotImplementedError()

    def clear(self):
        """Clear memory"""
        raise NotImplementedError()


class LocalMemory(_Memory):
    """Stores memory in the local session"""

    @property
    def engine(self):
        return self._engine

    @engine.setter
    def engine(self, engine):
        self._engine = self._def.check(engine)

    def load(self, **kwargs):
        """Return data stored in source variable

        If data can be coppied, it will. This might not be memory efficient,
        but it makes behaviour from the Memory._source attribute more
        consistent with external memory sources.
        """
        return copy(self.engine)

    def save(self, data):
        """Store input data into source variable"""
        self.engine = data

    def clear(self):
        self._engine = None
