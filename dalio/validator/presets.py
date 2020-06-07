from dalio.base.constants import ATTRIBUTE, PRICE, TICKER
from dalio.validator import IS_PD_TS, IS_PD_DF, HAS_COLS, HAS_INDEX_NAMES


STOCK_STREAM = [
    IS_PD_TS(), 
    IS_PD_DF(), 
    HAS_INDEX_NAMES([ATTRIBUTE, TICKER], axis=1)
]


STOCK_INFO = [
    IS_PD_DF(),
    HAS_COLS(TICKER)
]
