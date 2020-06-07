import sys
import warnings

import pandas as pd
import pandas_datareader as web
import matplotlib.pyplot as plt


def get_and_plot(source, cleaner):

    queue = sys.argv[2:]

    data = cleaner(web.DataReader(queue, source))

    print(data.iloc[-1])

    data.apply(lambda x: x / data.iloc[0].values * 100, axis=1).plot()

    # data.to_csv(f"stock_data_{pd.Timestamp.now()}.csv")

    plt.show()


def yahoo(data):
    return data.Close


def fred(data):
    return data


if __name__ == "__main__":

    warnings.filterwarnings("ignore")

    try:
        source = sys.argv[1] 
    except IndexError:
        source = "yahoo"

    if (source == "yahoo"):
        get_and_plot("yahoo", yahoo)
    elif (source == "fred"):
        get_and_plot("fred", fred)
    else:
        print("Invalid Source")

