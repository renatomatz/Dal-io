import pandas as pd

from dalio.base.constants import CATEGORY, IS_DELISTED, SCALE, \
        SIC_CODE, TICKER


def get_comps_by_sic(data, ticker, max_ticks=None):
    """Get an equity's comps based on market cap and sic code similarity
    """
    # select most recent data
    # data not has a different format, with tickers as its index
    data = data.groupby(TICKER)\
        .apply(lambda group: group.iloc[0])

    try:
        # get row with ticker data
        ticker_data = data[data[TICKER] == ticker]
        # filter so that ticker is not its own comps
        data = data[data[TICKER] != ticker]
    except KeyError:
        raise KeyError("Ticker does not exist in dataset")

    # If scale column is present, get companies with similar scale
    # TODO: delete this and change to additional filtering options
    # if SCALE in data.columns:
    #     # keep only scale category number
    #     data[SCALE] = data[SCALE].apply(lambda n: int(str(n)[0]) \
    #         if n is not None else None)

    #     ticker_cap = int(ticker_data[SCALE])

    #     # keep only data of companies with similar market cap
    #     data = data[data[SCALE] >= ticker_cap-1][data[SCALE] <= ticker_cap+1]
    
    # such that one digit is revealed at a time
    i = 0
    comps = pd.DataFrame()

    while (len(comps) < 3) and (i <= 3):

        # make sic code become broader until there are at least three comps
        # or first sic code digit
        sic_subset = (int(ticker_data[SIC_CODE]) // (10**i))
        sic_data = data[SIC_CODE].apply(lambda x: x // (10**i) if x else x)
        comps = data[sic_subset == sic_data]

        i += 1
     
    # if too many comps and {max_ticks} specified, return a random sample
    # of {max_ticks} rows
    if max_ticks is not None and len(comps) > max_ticks:
        # TODO: get additional filtering options
        # this filtering will be based on specified columns and their dtypes
        # if too many tickers, get similar categories/numbers etc
        comps = comps.sample(max_ticks, random_state=42)

    return [ticker] + comps.ticker.to_list()


# TODO: implement this function to filter data for similar numbers
def _similar_number(data, col, n, range=0.2):
    pass


# TODO: implement this function to filter data for similar categories
def _similar_category(data, col, cat):
    pass
