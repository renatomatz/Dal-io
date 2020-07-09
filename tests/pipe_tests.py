import sys
sys.path.append("/home/renatomz/Documents/Projects/Dal-io")

import numpy as np

from dalio.base.constants import *
from dalio.ops import *
from dalio.external import * 
from dalio.translator import *
from dalio.pipe import *
from dalio.model import *
from dalio.application import *

# File Input
file_in = StockStreamFileTranslator()(PandasInFile("tests/MGT441.xlsx"))
ticker = ["NVDA", "RL", "GPS", "WMT"]

# Yahoo Input
y_api = YahooDR()
y_in = YahooStockTranslator()(y_api)

tickers = ["MSFT", "AAPL"]
y_data_raw = y_in.run(ticker=tickers)

cols=["close", "high"]
y_change = Change(cols=cols)(y_in)
res = y_change.run(ticker=tickers)

y_col_select = ColSelect(cols)(y_in)
res = y_col_select.run(ticker=tickers)

y_custom = Custom(lambda x: x / x)(y_in)
res = y_custom.run(ticker=tickers)

risk_cust = Custom(risk_metrics, 0.94)(y_change)
res = risk_cust.run(ticker=tickers)

y_index = Index(index_at=100, cols=cols)(y_in)
res = y_index.run(ticker=tickers)

y_period = Period(period="Y")(y_in)
res = y_period.run(ticker=tickers)


# Quandl Input
# q_api = QuandlAPI("/home/renatomz/Documents/Projects/Dal-io/dalio/external/config/quandl_config.json")
# q_api.authenticate()

# q_sf1_in = QuandlSharadarSF1Translator()(q_api)
# q_tick_in = QuandlTickerInfoTranslator()(q_api)

# tickers = ["MSFT", "AAPL", "IBM", "TSLA", "XOM", "BP", "JPM"]

# q_data_raw = q_sf1_in.run(ticker=tickers)

# cols=["evebitda", "fcf", "price"]
# q_change = Change(cols)(q_sf1_in)
# q_col_select = ColSelect(cols)(q_sf1_in)
# q_custom = Custom(lambda x: 42)(q_sf1_in)
# q_index = Index(index_at=100, cols=cols)(q_sf1_in)
# q_period = Period(period="Y")(q_sf1_in)

# q_comps = StockComps(strategy="sic_code", max_ticks=None)(q_tick_in)

# q_data_change = q_change.run(ticker=tickers)
# q_data_col_select = q_col_select.run(ticker=tickers)
# q_data_custom = q_custom.run(ticker=tickers)
# q_data_index = q_index.run(ticker=tickers)
# q_data_period = q_period.run(ticker=tickers)
# q_data_comps = q_comps.run(
#     ticker="MSFT", 
#     filters={"table":"SF1"}
# )

# q_comps_financials = CompsFinancials()("comps_in", q_comps)("data_in", q_sf1_in)
# q_data_comps_financials = q_comps_financials.run(ticker="MSFT")

# q_comps_prices = q_comps_financials.with_input("data_in", y_in)
# q_data_comps_prices = q_comps_prices.run(ticker="MSFT")

# q_comps_printer = Printer()\
#     .set_input("data_in", q_sf1_in)\
#     .set_output("data_out", FileWriter())
# q_comps_printer.run(ticker=tickers)

# q_comps_grapher = PandasTSGrapher(y=CLOSE)\
#     .set_input("data_in", y_in)\
#     .set_output("data_out", PyPlotGraph())

# q_comps_grapher.run(ticker=tickers)

adj_close_in = ColSelect(columns="adj_close")(y_in)
returns = StockReturns(columns="adj_close")(adj_close_in)

garch = MakeARCH()(returns)\
    .set_piece("mean", "ARX", lags=[1, 3, 12])\
    .set_piece("volatility", "ARCH", p=5)\
    .set_piece("distribution", "StudentsT")

am = garch.run(ticker="MSFT")
am.fit()
var = ValueAtRisk(quantiles=[0.1, 0.01, 0.05])(garch)
var_res = var.run(ticker="MSFT")

pyplot_grapher = PyPlotGraph()

var_graph = VaRGrapher()\
    .set_input("data_in", var)\
    .set_output("data_out", pyplot_grapher)

fig = var_graph.run(ticker="MSFT")

S = CovShrink()(adj_close_in)
S.set_piece("shrinkage", "ledoit_wolf")

mu = ExpectedReturns()(adj_close_in)
mu.set_piece("return_model", "mean_historical_return")

S_data = S.run(ticker=tickers)
mu_data = mu.run(ticker=tickers)

port_ef = OptimumWeights()\
    .set_input("sample_covariance", S)\
    .set_input("expected_returns", mu)\
    .add_objective("L2_reg")\
    .add_constraint(lambda x: x[1] <= 0.5)\
    .add_stock_weight_constraint(
        ticker="MSFT",
        comparisson="==",
        weight=0.3
    )\
    .set_piece("strategy", "max_sharpe")

# ef = port_ef.run(ticker=tickers)

opt_port = OptimumPortfolio()\
    .set_input("data_in", adj_close_in)\
    .set_input("weights_in", port_ef)

port = opt_port.run(ticker=tickers)

lm = LinearModel()(adj_close_in)\
    .set_piece("strategy", "LinearRegression")

lm_fitted = lm.run(ticker="MSFT")

port_cla = MakeCriticalLine()\
    .set_input("sample_covariance", S)\
    .set_input("expected_returns", mu)

cla = port_cla.run(ticker=tickers)

time = DateSelect()
price = time.set_input(y_in)

# Select
pipe = ColDrop("close").set_input(price)
res = pipe.run(ticker=ticker)

val_drop = ValDrop(12855900, cols="volume").set_input(price)
res = pipe.run(ticker=ticker)

pipe = ValKeep(12855900, cols="volume").set_input(price)
res = pipe.run(ticker=ticker)

pipe = ColRename({"close": "CLOSE"}).set_input(price)
res = pipe.run(ticker=ticker)

pipe = DropNa().set_input(val_drop)
res = pipe.run(ticker=ticker)

pipe = FreqDrop(3, [("adj_close", "NVDA")]).set_input(price)
res = pipe.run(ticker=ticker)

pipe = ColReorder({"open":0}, level=0).set_input(price)
res = pipe.run(ticker=ticker)

pipe = RowDrop({("adj_close", "NVDA"): (lambda x: x < 100)}).set_input(price)
res = pipe.run(ticker=ticker)

# Generate
pipe = Bin({"NVDA": 3}, level=1, drop=False).set_input(price)
res = pipe.run(ticker=ticker)

pipe = OneHotEncode("is_delisted").set_input(q_tick_in)
res = pipe.run(ticker=ticker)

pipe = MapColVals("is_delisted", {1:"delisted", 2:"not delisted"}).set_input(q_tick_in)
res = pipe.run(ticker=ticker)

pipe = ApplyToRows((lambda row: row["open"] - row["close"]), "diff").set_input(price)
res = pipe.run(ticker=ticker)

pipe = ApplyByCols(("adj_close", "NVDA"), (lambda x: x*100)).set_input(price)
res = pipe.run(ticker=ticker)

pipe = ColByFrameFunc("Sum", (lambda x: x.apply(np.sum, axis=1))).set_input(price)
res = pipe.run(ticker=ticker)

pipe = AggByCols(("adj_close", "NVDA"), np.log).set_input(price)
res = pipe.run(ticker=ticker)

pipe = Log().set_input(price)
res = pipe.run(ticker=ticker)


# SKLearn
pipe = Encode().set_input(price)
res = pipe.run(ticker=ticker)

pipe = Scale().set_input(price)
res = pipe.run(ticker=ticker)


print("DONE")
