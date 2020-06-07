'''Example of setting up a comps analysis printout and graph'''
import numpy as np

import sys
sys.path.append("/home/renatomz/Documents/Projects/dalio")

from dalio.base.constants import CLOSE, EARNINGS, EV_EBITDA, FCF
from dalio.external import QuandlAPI, YahooDR
from dalio.translator import QuandlSharadarSF1Translator, YahooStockTranslator
# from dalio.pipe import StockFinancials, StockComps, Index, Monthly
# from dalio.model import JoinStocksDaily, CompsAnalysis
# from dalio.application import CompsPrinter, CompsTSGrapher

# Define constants
COLOUR_SCHEME = ["red", "green", "blue", "orange"]
COLS = [EARNINGS, EV_EBITDA, FCF]
TICK = "NKE"

# Setup connections to external sources
quandl_api = QuandlAPI("/home/renatomz/Documents/Projects/dalio/dalio/external/config/quandl_config.json")  # Quandl.com
quandl_api.authenticate()
yahoo_api = YahooDR()  # Yahoo! Finance
yahoo_api.authenticate()

'''Translate Quandl API calls into an accepted format, mostly standardizing
column names'''
quandl_stock_in = QuandlSharadarSF1Translator()(quandl_api)
'''Translate Yahoo! Finance API calls to fit accepted format, same as above'''
yahoo_stock_in = YahooStockTranslator()(yahoo_api)

test1 = quandl_stock_in.run(ticker="TSLA")
test2 = yahoo_stock_in.run(ticker="TSLA")

# Set up data extractors
'''Extract market close prices from the Yahoo! Finance data'''
extract_prices = StockFinancials(CLOSE).set_input(yahoo_stock_in)
'''Extract data specified on the COLS constant from the quandl API'''
extract_financials = StockFinancials(COLS).set_input(quandl_stock_in)
'''Extract a stock's comparative companies based on their siccode'''
extract_comps = StockComps(max_comps=6, by="siccode")\
        .set_input(quandl_stock_in)

# Set up comps printer
'''Join quandl and yahoo data, matching by stock (by default) and day
aggregate multiple values by picking the latest of them
the utils.latest function can be defined by the user'''
joined_financials = JoinStocksDaily(how="inner", agg_func=utils.latest)\
        .set_input("left", extract_prices)\
        .set_input("right", extract_financials)

'''Build a comps model using the comps extractor to get a company's' comps
and the joint financial data extractor to get the comparative data'''
comps_model = CompsAnalysis(agg_func=np.mean)\
        .set_input("financials", joined_financials)\
        .set_input("comps", extract_comps)

'''Set up the printer application'''
comps_printer = CompsPrinter().set_input(comps_model)

# Set up comps graphers
'''Set up a monthly period aggregator that takes in data from an indexer'''
to_monthly = Monthly(agg_func=utils.latest, fill_func=np.mean)\
        .set_input(Index(100, by="ticker").set_input(extract_prices))
'''fast set up of an yearly aggregator using the same attributes and input
as to_monthly'''
to_yearly = to_monthly.make("yearly")

'''Create a grapher using monthly, indexed financial data from comps extracted
from a stock. Data starting on 2007'''
monthly_grapher = CompsTSGrapher()\
        .set_color_scheme(COLOUR_SCHEME)\
        .set_start("2007")\
        .set_input("price", to_monthly)\
        .set_input("comps", extract_comps)

'''Fast creation of an yearly graph with the same attributes as monthly_grapher
but input of yearly, index financial data'''
yearly_grapher = monthly_grapher.with_input("price", to_yearly)

# Execute applications from shell
if __name__ == "__main__":
    '''Execute models on the chosen ticker symbol'''
    comps_printer.execute(TICK)
    monthly_grapher.execute(TICK)
    yearly_grapher.execute(TICK)
