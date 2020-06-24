"""Define quandl external request classes

Quandl has a varied set of tables to request data from, so these classes just
implement basic functionaliy to import data from any table given quandl's
basic functionalities.
"""
import os
import quandl

from dalio.external import External


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
