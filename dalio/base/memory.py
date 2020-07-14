"""Defines memory transformers"""

from copy import copy
from collections import deque

from dalio.base import _Transformer, _DataDef


class Memory(_Transformer):
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
        _source (any): Memory source. Implementations will often have
            additional attributes to manage this source.
    """

    def __init__(self):
        super().__init__()
        self._def = _DataDef()
        self.clear()

    def run(self, **kwargs):
        """Check if location is set and return stored data accordingly"""
        raise NotImplementedError()

    def copy(self, *args, **kwargs):
        """Create new instance and memory source"""
        ret = type(self)(*args, **kwargs)
        ret.set_input(self._source)
        ret.add_tag(self._tags)
        return ret

    def set_input(self, new_input):
        """Store input data"""
        raise NotImplementedError()

    def with_input(self, new_input):
        return self.copy().set_input(new_input)

    def clear(self):
        """Clear memory"""
        raise NotImplementedError()


class LocalMemory(Memory):
    """Stores memory in the local session"""

    def run(self, **kwargs):
        """Return data stored in source variable

        If data can be coppied, it will. This might not be memory efficient,
        but it makes behaviour from the Memory._source attribute more
        consistent with external memory sources.
        """
        return copy(self._source)

    def set_input(self, new_input):
        """Store input data into source variable"""
        self._source = self._def.check(new_input)
        return self

    def clear(self):
        self._source = None


class LazyRunner(_Transformer):
    """Memory manager created to set memory input of an object after
    executing a transformer with given kwargs.

    This is useful when you want to store data sourced from a transformer but
    doesn't know which kwarg requests will be used. This object waits for
    a run request to source data from a transformer and set the source of
    one or more memory objects (in order) when data does arrive, and reutilize
    it if data is requested with the same kwargs.

    For every new and valid evaluation, a new Memory instance is created and
    saved as a value in the _memory dictionary. Failed memory storage will
    result in no new Memory instance being created. This is done instead of
    simply setting inputs to Memory instances created upon initialization in
    order to reduce the memory usage of LazyRunner instances.

    KEEP IN MIND that this does not check if the actual input is the same to
    relay the data, only the kwargs (for speed's sake). This creates the risk
    that inputs or transformer attributes are changed (keeping kwarg requests
    the same) and the old data is retrieved. Use the pop() or clear() methods
    to solve this.

    KEEP IN MIND there is a risk of having very different inputs being
    retrieved (from different external sources or date filters, for example)
    only based on kwarg requests. This is only relevant if _buff > 1.


    Attributes:
        _source (_Transformer): transformer to source data from. No data
            definitions are used as this should be performed by the memory
            type uppon setting an input.
        _mem_type (type): type object for generating new memory instances.
        _args (tuple): tuple of arguments for new _mem_type instance
            initialization
        _kwargs (dict): dict of keyword arguments for new _mem_type instance
            initialization
        _memory (deque): deque containing one Memory instance for
            every unique kwargs ran with a (kwarg, Memory) tuple structure.
        _buff (int, -1 or >0): Maximum number of Memory instances to be stored
            at any point. Positive numbers will be this limit, -1 represent no
            buffer limits. This option should be used with caution, as it can
            be highly memory-inneficient. Subclasses can create new methods of
            managing this limit.
        _update (bool): Whether _memory dict should be updated if a new
            set of kwargs is ran after reaching maximum capacity (as defined
            by the _buff attribute). If set to True, the last element of the
            _memory dict will be substituted.
    """

    def __init__(self,
                 mem_type,
                 args=None, kwargs=None,
                 buff=1,
                 update=False):
        """Check parameter data types, simplify parameters and initialize
        instance

        Args:
            mem_type (type): Memory type to be created.
            args (iterable): None by default. Arguments for mem_type
                initialization. If None, empty tuple is used.
            kwargs (dict-like): None by default. Keyworkd arguments for
                mem_type initialization. If None, empty dict is used.
            buff (int): Buffer size. If <=0, -1 will be used to represent
                no buffer limit.
            update (bool): whether to update memory dict once buffer limit is
                reached.

        Raises:
            TypeError if mem_type argument is not of type type.
        """
        super().__init__()

        if isinstance(mem_type, type):
            self._mem_type = mem_type
        else:
            raise TypeError("mem_type should be of type {type}, not \
                {type(mem_type)}")

        self._args = args if args is not None else tuple()
        self._kwargs = kwargs if kwargs is not None else dict()

        self.set_buff(buff)
        self.set_update(update)

        self.clear()

    def run(self, **kwargs):
        """Compare kwargs with existing keys, update or set _memory in
        accordance to _update and _buff attributes.

        Raises:
            BufferError: if new kwargs, buffer is full and update set to False
        """
        for kw, mem in self._memory:
            if kw == kwargs:
                return mem.run(**kwargs)
                # leave the for loop and skip the else statement
                break
        else:
            # this is only executed if kwargs is not in memory list
            if len(self._memory) >= self._buff and self._buff != -1:

                if self._update:
                    self._memory.popleft()
                else:
                    raise BufferError("Memory buffer is full and is not set \
                        to be updated")

            data = self._source.run(**kwargs)

            new_mem = self._mem_type(*self._args, **self._kwargs)\
                .set_input(data)

            # create a (kwargs, memory) tuple
            self._memory.append((kwargs, new_mem))

            return data

    def copy(self, *args, **kwargs):
        """Return a copy of this instance with a shallow memory dict copy"""
        ret = type(self)(
            self._mem_type,
            *args,
            args=self._args, kwargs=self._kwargs,
            buff=self._buff,
            update=self._update,
            **kwargs
        ).set_input(copy(self._source))
        ret._memory = self._memory.copy()
        return ret

    def set_input(self, new_input):
        """Set the input data source.

        Args:
            new_input (_Transformer): new transformer to be set as input.

        Raises:
            TypeError: if new_input is not an instance of _Transformer.
        """
        if isinstance(new_input, _Transformer) or new_input is None:
            self._source = new_input
        else:
            raise TypeError(f"new input must be a _Transformer \
                instance, not {type(new_input)}")

        return self

    def with_input(self, new_input):
        return self.copy().set_input(new_input)

    def clear(self):
        """Clear memory"""
        self._memory = deque()

    def set_update(self, update):
        """Set the _update attribute"""
        self._update = update

    def set_buff(self, buff):
        """Set the _buff attribute"""
        self._buff = buff if buff > 0 else -1
