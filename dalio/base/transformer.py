'''Define Transformer class'''

from typing import Set
from dalio.base import _Node


class _Transformer:
    '''Transformers are a base class that represents any kind of data
    modification. These interact with
    DataOrigin instances as they are key to their input and output integrity.
    A set_source() method sets the source of the input, the .run() method
    cannot be executed if the input's source is not set.

    === Attributes ===

    _source: source Node with connection to data input. Notice that while the
    Node stored here has a _connection attribute, we will refer to it as an
    input only, which is the case for base transformers and allows users to
    picture creating connections better.

    _req_args: set of unique required args. This has a default value set on
    initialization for this specific Transformer's required arguments but is
    also updated for every new input set to reflect their required arguments.
    This should be used sparingly for absolutely-necessary arguments.

    === Methods ===

    run: get data from source and transform it
    - return: modified data

    copy: make a deep copy of the object
    - return: object of the ssame instance

    get_input: get input connection
    - return: Transformer

    set_input: set the input connection to the source

    with_input: return a copy of this instance with a given connection as
    the input to its source
    - return: Transformer

    req_args: return a list of arguments needed for the run method

    __call__: same as with_input()

    '''

    _source: _Node
    _req_args: Set[str]

    def __init__(self):
        '''Initialize Transformer object by creating necessary DataOrigin
        descriptions and set these input(s) data source
        '''
        self._source = None
        self._req_args = set()

    def run(self, **kwargs):
        '''Get data from source and run the transformation
        '''
        raise NotImplementedError()

    def copy(self):
        '''Make a deep copy of transformer
        '''
        raise NotImplementedError()

    def get_input(self):
        return self._source.get_connection()

    def set_input(self, new_input):
        '''Set the input connection in-place
        '''
        raise NotImplementedError()

    def with_input(self, new_input):
        '''Return copy of this transformer with the new data connection
        '''
        raise NotImplementedError()

    def req_args(self):
        '''Return the unique arguments needed for the run method
        '''
        return set(self._req_args).union(self.get_input().req_args())

    def __call__(self, new_input):
        '''Easier interface to with_input
        '''
        return self.set_input(new_input)
