from dalio.model import Model
from dalio.validator import IS_TYPE, ELEMS_TYPE, IS_PD_DF, STOCK_INFO, \
        STOCK_STREAM
from dalio.base.constants import TICKER


class CompsData(Model):

    def __init__(self):
        super().__init__()

        self._init_source([
            "data_in",
            "comps_in"
        ])

        self._get_source("comps_in")\
            .add_desc(IS_TYPE(list))\
            .add_desc(ELEMS_TYPE(str))
        
        self._get_source("data_in")\
            .add_desc(IS_PD_DF())

    def run(self, **kwargs):
        ticker = self._source_from("comps_in", **kwargs)
        kwargs[TICKER] = ticker
        return self._source_from("data_in", **kwargs)


class CompsInfo(CompsData):

    def __init__(self):
        super().__init__()
        
        self._get_source("data_in")\
            .add_desc(STOCK_INFO)


class CompsFinancials(CompsData):

    def __init__(self):
        super().__init__()
        
        self._get_source("data_in")\
            .add_desc(STOCK_STREAM)