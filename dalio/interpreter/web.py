"""Define web external request classes"""

import os

import json

import requests

import quandl
import pandas_datareader.data as web

from dalio.interpreter import _Interpreter

class _Web(_Interpreter):
    """Represents external data coming from a web API

    This is a base representation for functionaliy common to most apis.

    Attributes:
        _engine (any): API engine
        path_to_config (str): path to a config file containing configuration
            details. File must be of .json format
        _api_key (str): API key for engine authentication. Can be either set
            manually or be in an "api_key" key in a specified config file.
        _api_secret (str): API secret for engine authentication. Can be
            either set manually or be in an "api_secret" key in a specified
            config file.
    """

    def __init__(self, path_to_config=None, api_key=None, api_secret=None):
        """Initializes instance and assigns a datareader instance to the
        connection.
        """
        super().__init__()

        if path_to_config is not None and not os.path.exists(path_to_config):
            raise IOError("path to config file does not exist")

        self.path_to_config = path_to_config

        self._api_key = api_key
        self._api_secret = api_secret

        self.clear()

    @property
    def api_key(self):
        """Get api key from variable if it is set, or else from a file"""
        if self._api_key is None and self.path_to_config is not None:
            try:
                with open(self.path_to_config) as f:
                    return json.load(f)["api_key"]
            except KeyError:
                raise KeyError("config file does not have an 'api_key' key")

        return self._api_key

    @api_key.setter
    def api_key(self, api_key):
        self._api_key = api_key

    @property
    def api_secret(self):
        """Get api secret from variable if it is set, or else from a file"""
        if self._api_secret is None and self.path_to_config is not None:
            try:
                with open(self.path_to_config) as f:
                    return json.load(f)["api_secret"]
            except KeyError:
                raise KeyError("config file does not have an 'api_secret'\
                    key")

        return self._api_key

    @api_secret.setter
    def api_secret(self, api_secret):
        self._api_secret = api_secret

    def request(self, query, *args, **kwargs):
        """Send generic request

        Args:
            query (str): request string
            *args, **kwargs: request function parameters. Function calls
                might be dependent on some of these.
        """
        raise NotImplementedError()


class BaseDR(_Web):
    """Represents external data coming from the pandas_datareader package

    This is a base representation for functionaliy common to DataReader
    instances, but mostly used as a base class.
    """

    def __init__(self, path_to_config=None, api_key=None):
        """Initializes instance and assigns a datareader instance to the
        connection.
        """
        super().__init__(path_to_config=None, api_key=None)
        self.clear()

    @property
    def engine(self):
        return self._engine

    @engine.setter
    def engine(self, engine):
        if callable(engine):
            self._engine = engine
        raise AttributeError("engine must be callable")

    def clear(self):
        self.engine = web.DataReader

    def request(self, query, *args, **kwargs):
        """Get data from a pandas-datareader source

        KEEP IN MIND calling this from PDRBase requires a source in the *args
        """
        return self.engine.__call__(query, *args, **kwargs)

    @staticmethod
    def from_source(source, *args, **kwargs):
        """Make a _PDR subclass instance from a specified source name

        Args:
            source (str): name of the pandas-datareader source.
            *args, **kwargs: initialization parameters.
        """

        if source == "yahoo":
            return YahooDR(*args, **kwargs)

        raise ValueError(f"{source} is not an available source")


class YahooDR(BaseDR):
    """Represents financial data from Yahoo! Finance"""

    def request(self, query, *args, **kwargs):
        return self.engine.__call__(query, "yahoo", *args, **kwargs)


class QuandlAPI(_Web):
    """Set up the Quandl API and request table data from quandl."""

    def __init__(self, path_to_config=None, api_key=None):
        """Initialize instance and set the api key if possible"""
        super().__init__(path_to_config=None, api_key=None)

        self.clear()

    @property
    def engine(self):
        return self._engine

    def clear(self):
        self._engine = quandl

    def request(self, query, *args, **kwargs):
        """Request table data from quandl

        Args:
            query (str): table to get data from.
            *args, **kwargs: keyword arguments for quandl request.

        Raises:
            IOError: if api key is not set.
            ValueError: if filters kwarg is not a dict.
        """
        if not self.check():
            raise IOError("Connection is not valid")

        return self.engine.get_table(query,
                                     *args,
                                     paginate=True,
                                     **kwargs)

    def check(self):
        """Check if the api key is set"""
        if self.engine.ApiConfig.api_key is None:
            return self.authenticate()

        return True


    def authenticate(self):
        """Set the api key if it is available in the config dictionary

        Returns:
            True if key was successfully set, False otherwise
        """
        self.engine.ApiConfig.api_key = self.api_key

        return True
