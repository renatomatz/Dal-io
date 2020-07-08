"""Define web external request classes"""

import os

import quandl
import pandas_datareader as web

from dalio.external import External


class _PDR(External):
    """Represents external data coming from the pandas_datareader package

    This is a base representation for functionaliy common to DataReader
    instances, but mostly used as a base class.
    """

    def __init__(self, config=None):
        """Initializes instance and assigns a datareader instance to the
        connection.
        """
        super().__init__(config)
        self.set_connection(web.DataReader)

    def request(self, **kwargs):
        """See base class"""
        raise NotImplementedError()

    def make(self, name=None):
        """Make an instance of a specified subclass name"""
        ret = type(self)()

        if name == "yahoo":
            ret = YahooDR()

        ret.update_config(self._config)
        return ret


class YahooDR(_PDR):
    """Represents financial data from Yahoo! Finance"""

    def request(self, **kwargs):
        """Get data from specified tickers"""
        return self._connection(kwargs["ticker"], "yahoo")


class QuandlAPI(External):
    '''Set up the Quandl API and request table data from quandl.

    Attributes:
        _quandl_conf: Quandl API config object
    '''

    def __init__(self, config=None):
        """Initialize instance and set config file to a default"""

        if config is None and os.path.exists("config/quandl_config.json"):
            config = "quandl_config.json"

        super().__init__(config)

        self.set_connection(quandl.ApiConfig)

    def request(self, **kwargs):
        """Request table data from quandl

        Args:
            **kwargs: keyword arguments for quandl request.
                query: table to get data from.
                filter: dictionary of filters for data. Depends on table.
                columns: columns to select.

        Raises:
            IOError: if api key is not set.
            ValueError: if filters kwarg is not a dict.
        """
        if not self.check():
            raise IOError("Connection is not valid")

        filters = kwargs.get("filters", {})

        if not isinstance(filters, dict):
            raise ValueError("'filters' keyword argument must be a \
                dictionary of valid Qaudnl column filters")

        # set up queue options
        qopts = dict()

        if "columns" in kwargs:
            qopts["columns"] = kwargs["columns"]

        return quandl.get_table(kwargs["query"],
                                ticker=kwargs.get("ticker", None),
                                paginate=True,
                                qopts=qopts,
                                **filters)

    def check(self):
        """Check if the api key is set"""
        if self._connection.api_key is None:
            return self.authenticate()
        else:
            return True

    def authenticate(self):
        """Set the api key if it is available in the config dictionary

        Returns:
            True if key was successfully set, False otherwise
        """
        if "api_key" in self._config:
            self._connection.api_key = self._config["api_key"]
            return True
        else:
            return False
