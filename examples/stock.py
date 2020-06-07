import sys
import warnings

import quandl

# import pandas as pd
# import pandas_datareader as web
# import matplotlib.pyplot as plt

quandl.ApiConfig.api_key = "RC1Vo694qxsA7ryV4Cii"

ticker = sys.argv[1:]

columns = ["calendardate", "ticker", "price",
           "ev", "ebitda", "evebitda", "grossmargin",
           "revenue", "fcf", "workingcapital"]

data = quandl.get_table("SHARADAR/SF1",
                        ticker=ticker,
                        qopts={"columns": columns})

data = data.groupby("ticker").apply(lambda x: x.iloc[0])

data.transpose()

print("\n")
print(data)

data.to_csv("comps_fundamentals.csv")

# warnings.filterwarnings("ignore")

# def index_ticks(df, ax):
    # ax.set_xticklabels(df.index.strftime("%m/%y")[::len(df.index)//11],
                       # rotation=0,
                       # ha="right")


# price = web.DataReader(ticker, "yahoo")
# evebitda = quandl.get_table("SHARADAR/DAILY",
                            # ticker=ticker,
                            # qopts={"columns": ["date", "evebitda"]}) \
                 # .set_index("date")

# fig = plt.figure()

# ax1 = fig.add_subplot(211)
# ax1.plot(price.index, price.Close)
# ax1.set_xlabel(None)
# index_ticks(price, ax1)
                   
# ax2 = fig.add_subplot(212)
# ax2.plot(evebitda.index, evebitda.evebitda)
# index_ticks(evebitda, ax2)

# plt.show()
# # plot prices
