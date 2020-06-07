import os
import quandl

from dalio.external import External


class QuandlAPI(External):
    '''Set up the Quandl API

    === Attributes ===
    _quandl_conf: Quandl API config object
    '''

    def __init__(self, config=None):

        if config is None and os.path.exists("config/quandl_config.json"):
            config = "quandl_config.json"

        super().__init__(config)

        self.set_connection(quandl.ApiConfig)

    def request(self, **kwargs):

        self.check()

        if "filters" in kwargs and not isinstance(kwargs["filters"], dict):
            raise ValueError("filters keyword argument must be a dictionary of\
                    valid Qaudnl column filters")

        # set up queue options
        qopts = dict()

        if "columns" in kwargs:
            qopts["columns"] = kwargs["columns"]

        return quandl.get_table(kwargs["query"],
                                ticker=kwargs.get("ticker", None),
                                paginate=True,
                                qopts=qopts,
                                **kwargs.get("filters", {}))

    def check(self):
        return self._connection.api_key is not None

    def authenticate(self):
        if "api_key" in self._config:
            self._connection.api_key = self._config["api_key"]
        else:
            raise IOError()
