'''Define abstract External class'''

import json
from typing import Any, Dict

from dalio.base import _Node


class External(_Node):
    '''External instances manage connections between your environment and an
    external source. Class instacnes will often be redundant with existing
    connection handlers, but at least subclasses will allow for more
    integrated connection handling and collection, so that you can have a
    single connection object for each external connection.

    === Attributes ===

    _connection: connection with outside source of data

    _config: authentication settings for outside sources

    === Methods ===

    authenticate: authenticate connection
    - return: True if authenication is successful or if it is already existent
    False if the authentication fails

    '''

    _connection: Any
    _config: Dict[str, Any]

    def __init__(self, config=None):
        super().__init__()

        if config is not None:
            if ".json" in config:
                with open(config) as f:
                    self._config = json.load(f)
            elif isinstance(config, dict):
                self._config = config
            else:
                raise ValueError("config must be a file or dictionary")
        else:
            self._config = dict()

    def request(self, **kwargs):
        '''Send a request to a connection
        '''
        raise NotImplementedError()

    def check(self):
        '''Check connection, return whether data is available to be sourced
        '''
        raise NotImplementedError()

    def authenticate(self):
        '''Establish a connection with the source, does nothing by default
        '''
        pass
