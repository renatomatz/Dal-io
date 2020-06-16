'''Define Model class'''

from typing import Dict
from dalio.base import _Transformer
from dalio.datadef import _DataDef


class Model(_Transformer):
    '''Models are a lot like transformers as they take in inputs and has a
    single output. Models do differ from transformers as they can take in
    multiple inputs and be much more flexible with additional options for
    different strategies or for limited data storage.

    === Attributes ===

    _source: dictionary of input data definitions

    === Method ===

    '''

    _source: Dict[str, _DataDef]

    def __init__(self):
        '''Set up initial inputs and DataDefs, you won't get to do this later
        '''
        super().__init__()
        self._source = {}

    def copy(self, *args, **kwargs):
        # TODO: make all copy() methods take in args and kwargs and call super
        ret = type(self)(*args, **kwargs)

        for name, node in self._source.items():
            ret._source[name] = node

        return ret

    def set_input(self, input_name, new_input):
        '''Set a new input to data definition in dictionary entry
        matching the name
        '''

        try:
            self._source[input_name].set_connection(new_input)
        except KeyError:
            raise KeyError()  # TODO: Make better exceptions

        return self

    def with_input(self, input_name, new_input):
        '''Return a copy of this model with the specified data definition
        input changed
        '''
        return self.copy().set_input(input_name, new_input)

    def _source_from(self, name, **kwargs):
        '''Helper function to get data from a specified source
        '''
        if name in self._source:
            return self._source[name].request(**kwargs)
        else:
            raise KeyError(f"{name} not present in source dict, pick one of\
                {self._source.keys()}")

    def _get_source(self, name):
        try:
            return self._source[name]
        except KeyError:
            raise KeyError(f"specified source not in source list, specify \
                    one of: {self._source.keys()}")

    def _init_source(self, sources):
        if isinstance(sources, list):
            for source in sources:
                if isinstance(source, str):
                    self._source[source] = _DataDef()
                else:
                    raise ValueError("source names must be strings")
        else:
            raise ValueError("please specify a list of strings to the \
                    sources argument")

    def __call__(self, input_name, new_input):
        '''Friendlier interface for with_input call
        '''
        return self.set_input(input_name, new_input)
