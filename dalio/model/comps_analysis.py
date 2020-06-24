"""Define comps analysis models"""
from dalio.model import Model
from dalio.validator import IS_TYPE, ELEMS_TYPE, IS_PD_DF

from dalio.validator.presets import STOCK_INFO, STOCK_STREAM

from dalio.base.constants import TICKER


class CompsData(Model):
    """Get a ticker's comps and their data.

    This model has two sources: comps_in and data_in. comps_in gets a
    ticker's comparative stocks. data_in sources ticker data given a "TICKER"
    keyword argument.
    """
    def __init__(self):
        """Initialize instance.

        comps_in is defined as a list of strings.
        data_in is defined as a pandas dataframe.
        """
        super().__init__()

        self._init_source([
            "comps_in"
            "data_in",
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
    """Subclass to CompsData for getting comps stock information"""

    def __init__(self):
        """Initialize instance.

        data_in is also described as a stock info validator preset.
        """
        super().__init__()

        self._get_source("data_in")\
            .add_desc(STOCK_INFO)


class CompsFinancials(CompsData):
    """Subclass to CompsData for getting stock price information"""

    def __init__(self):
        """Initialize instance.

        data_in is also described as a stock stream validator preset.
        """
        super().__init__()

        self._get_source("data_in")\
            .add_desc(STOCK_STREAM)
