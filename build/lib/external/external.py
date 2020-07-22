"""Define abstract External class

External instances manage connections between your environment and an
external source. Class instacnes will often be redundant with existing
connection handlers, but at least subclasses will allow for more integrated
connection handling and collection, so that you can have a single connection
object for each external connection.
"""

import json
from typing import Any, Dict

from dalio.base import _Node


class External(_Node):
    """Represents external data input or output

    External instances have one external input and one internal output or one
    internal input and one external output.

    Attributes:
        _connection: connection with outside source of data
        _config (dict): authentication settings for outside sources
    """

    _connection: Any
    _config: Dict[str, Any]

    def __init__(self, config=None):
        """Initialize instance and get config from file

        Args:
            config: dictionary with configurations or file containing
                configuration settings translatable to a dictionary

        Raises:
            ValueError: if config is a non-existent file or not a dict.
        """
        super().__init__()

        self._config = dict()
        if config is not None:
            self.update_config(config)

    def request(self, **kwargs):
        """Request data to or from an external source
        """
        raise NotImplementedError()

    def check(self):
        """Check if connection is ready to request data

        Returns:
            Whether data is ready to be requested
        """
        return self._connection is not None

    def authenticate(self):
        """Establish a connection with the source.

        Returns:
            True if authenication is successful or if it is already existent
            False if the authentication fails.
        """
        return self._connection is not None

    def update_config(self, new_conf):
        """Update configuration dict with new data

        Args:
            new_conf: dictionary with new configurations or file containing
                configuration settings translatable to a dictionary

        Raises:
            TypeError: if config is a non-existent file or not a dict.
        """
        if ".json" in new_conf:
            with open(new_conf) as f:
                self._new_conf = json.load(f)
        elif isinstance(new_conf, dict):
            self._new_conf = new_conf
        else:
            raise TypeError("config must be a file or dictionary, \
                not {type(new_conf)}")
