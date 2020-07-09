import sys
sys.path.append("/home/renatomz/Documents/Projects/Dal-io")

import numpy as np
import pandas as pd

from dalio.external import *
from dalio.translator import *
from dalio.ops import *
from dalio.pipe import *

# File Input
f_in = StockStreamFileTranslator()(PandasInFile("tests/MGT441.xlsx"))

# Yahoo Input
y_api = YahooDR()
y_in = YahooStockTranslator()(y_api)

# Quandl Input
q_api = QuandlAPI("/home/renatomz/Documents/Projects/Dal-io/dalio/external/config/quandl_config.json")
q_api.authenticate()

q_sf1_in = QuandlSharadarSF1Translator()(q_api)
q_tick_in = QuandlTickerInfoTranslator()(q_api)

# Tests
base = Change("pct_change")(y_in)
ticker = ["NVDA", "RL", "GPS", "WMT"]
kwargs = {
    "columns": {1: "WMT"},
    "reintegrate": True,
}

basic = Custom(lambda x: x / x, **kwargs)(base)
var = Custom(risk_metrics, 0.94, **kwargs)(base)

kwargs.update({
    "new_cols": None,
    "reintegrate": False,
})

avg = Custom(np.mean, **kwargs)(base)
sd = Custom(np.std, **kwargs)(base)
cov = Custom(np.cov, **kwargs)(base)

rol_avg = Rolling(np.mean, columns={1:None}, rolling_window=10, new_cols="_new")(base)
# res = rol_avg.run(ticker=ticker)

for test in [basic, var, avg, sd, cov, rol_avg]:
    res = test.run(ticker=ticker)
    print(res)