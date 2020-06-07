import pandas_datareader as web

from dalio.external import External


class _PDR(External):

    def __init__(self, config=None):
        super().__init__(config)
        self.set_connection(web.DataReader)

    def make(self, name=None):
        ret = type(self)()

        if name == "yahoo":
            ret = YahooDR()

        ret.update_config(self._config)
        return ret

    def update_config(self, new_conf):
        if isinstance(new_conf, dict):
            self._config.update(new_conf)


class YahooDR(_PDR):

    def request(self, **kwargs):
        return self._connection(kwargs["ticker"], "yahoo")
