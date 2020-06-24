"""Define pandas_datareader external request classes"""

import pandas_datareader as web

from dalio.external import External

import warnings
warnings.filterwarnings("ignore")


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
