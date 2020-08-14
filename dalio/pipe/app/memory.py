"""Defines memory transformers"""

from collections import deque

from dalio import PipeApplication


class LazyRunner(PipeApplication):
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

        self._init_pieces([
            "mem_type"
        ])

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
                return mem.load(**kwargs)
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

            return self.transform(self._source.run(**kwargs), **kwargs)

    def transform(self, data, **kwargs):
        """Modify sourced data, create a new memory object and append it to 
        the memory attribute.
        """

        new_mem = self.build(data)

        # create a (kwargs, memory) tuple
        self._memory.append((kwargs, new_mem))

        return data

    def build(self, data, **kwargs):
            
        mem_type = self._pieces["mem_type"]
        if not isinstance(mem_type, type):
            raise TypeError("mem_type should be of type {type}, not \
                {type(mem_type)}")

        mem = mem_type.name(*mem_type.args, **mem_type.kwargs)
        mem.save(data)

        return mem

    def copy(self, *args, **kwargs):
        """Return a copy of this instance with a shallow memory dict copy"""
        ret = super().copy(
            self._mem_type,
            *args,
            args=self._args, kwargs=self._kwargs,
            buff=self._buff,
            update=self._update,
            **kwargs
        )
        ret._memory = self._memory.copy()

        return ret

    def clear(self):
        """Clear memory"""
        for mem in self._memory.values():
            mem.close()

        self._memory = deque()

    def set_update(self, update):
        """Set the _update attribute"""
        self._update = update

    def set_buff(self, buff):
        """Set the _buff attribute"""
        self._buff = buff if buff > 0 else -1
