"""Define constant terms

In order to maintain name integrity throughout graphs, constants are used 
instead of any string name for variables that were created or will be usued in
any _Transformer instance before or after the current one. These are often 
column names for pandas DataFrames, though can be anything that is or will be
used to identify data throughout the graph.
"""

ADJ_CLOSE = "adj_close"
ATTRIBUTE = "attribute"
CATEGORY = "category"
CLOSE = "close"
DATE = "date"
EARNINGS = "earnings"
EV_EBITDA = "ev_ebitda"
EXPECTED_SHORTFALL = "expected_shortfall"
FCF = "free_cash_flow"
HIGH = "high"
IS_DELISTED = "is_delisted"
LAST_UPDATED = "last_updated"
LOW = "low"
MAX_EXEDENCE = "max_exedence"
MEAN = "mean"
OPEN = "open"
PORTFOLIO = "portfolio"
PRICE = "price"
RESIDUAL_VARIANCE = "residual_variance"
SCALE = "scale"
SIC_CODE = "sic_code"
TICKER = "ticker"
RETURNS = "returns"
VARIANCE = "variance"
VOLATILITY = "volatility"
VOLUME = "volume"
