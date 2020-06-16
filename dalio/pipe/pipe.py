'''Defines the Pipe class'''

from typing import List
from functools import reduce

from dalio.base import _Transformer
from dalio.datadef import _DataDef


class Pipe(_Transformer):
    '''Pipes represend data modifications with one input and one output.

    === Attributes ===

    _source: input data definition

    === Methods ===

    transform: transform certain input

    pipeline: create new pipeline with isntace's source and any additional
    Pipe to be added

    __add__: return other pipe with this pipe set as input

    '''

    _input: _DataDef

    def __init__(self):
        '''Set up input DataDef
        '''
        super().__init__()
        self._source = _DataDef()

    def run(self, **kwargs):
        '''Get data from input, transform it, and return it '''
        return self.transform(self._source.request(**kwargs), **kwargs)

    def copy(self, *args, **kwargs):
        ret = type(self)(*args, **kwargs)
        ret._source = self._source
        return ret

    def transform(self, data, **kwargs):
        '''Apply a transformation to data returned from
        self._input.get(kwargs)
        '''
        return data

    def pipeline(self, *args):
        '''Return a PipeLine instance with self as the input source and any
        other Pipe instances as part of its pipeline
        '''
        return PipeLine(self, *args)

    def __add__(self, other):
        '''Return a Pipe object with this as its first stage
        '''
        return other.with_input(self)

    def set_input(self, new_input):
        '''Set the input data source in place
        '''
        if isinstance(new_input, _Transformer):
            self._source.set_connection(new_input)
        else:
            raise ValueError()  # TODO: better exceptions

        return self

    def with_input(self, new_input):
        '''Return copy of this transformer with the new data source
        '''
        return self.copy().set_input(new_input)


class PipeLine(Pipe):
    '''As Pipe instances hav one input and one output, PipeLine instances
    represent multiple of these transformations being done consecutively.
    You are able to join them together, thorugh the
    extend() and __add__ method to create a sequence of transformations
    linked one after the other.

    KEEP IN MIND:

    - order matters

    - validation is not performed on each stage of the pipeline, only on the
    input

    === Attributes ===

    pipeline: list of Pipe instaces this pipeline is composed of

    === Methods ===

    extend: extend the pipeline

    '''

    _pipeline: List[Pipe]

    def __init__(self, *args):
        '''Initialize PipeLine with initial Pipe instacnes being passed into
        *args
        '''
        super().__init__()
        self._source = args[0]._source

        self._pipeline = []
        self.extend(*args)

    def transform(self, data, **kwargs):
        '''Modify transform to pass input data on all pipes
        '''
        for pipe in self._pipeline:
            data = pipe.transform(data)

        return data

    def extend(self, *args):
        '''Extend existing pipeline with one or more Pipe instances
        '''
        for pipe in args:

            if isinstance(pipe, PipeLine):
                self._pipeline.extend(pipe.pipeline)
            elif isinstance(pipe, Pipe):
                self._pipeline.append(pipe)
            else:
                raise TypeError()  # TODO: make propper exceptions

    def __add__(self, other):
        return type(self).extend(self, other)
