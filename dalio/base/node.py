'''Defines Node abstract class'''


from typing import List


class _Node:
    '''Nodes are the key building blocks of your model as they represent any
    data that passes thorugh it. These are usued in subsequent classes to 
    describe and manage data.

    === Attributes ===

    _connection: entry point for data

    tags: strings to describe this node

    === Methods ===
   
    request: request data from a source

    check: check if sourced data is correct

    describe: output a description of the Node

    '''

    tags: List[str]

    def __init__(self):
        self._connection = None
        self.tags = []

    def request(self, **kwargs):
        raise NotImplementedError()

    def check(self):
        raise NotImplementedError()

    def describe(self):
        '''Describe this node
        '''
        print(f"\
        connection: {self._connection}\n \
        tags: {self.tags}\n \
        ")

    def get_connection(self):
        '''Get connection attribute
        '''
        return self._connection

    def set_connection(self, new_connection):
        '''Set connection attribute
        This method can be further extended to certify connection and implement
        other securuty features
        '''
        self._connection = new_connection
        return self
