![logo](https://github.com/renatomatz/Dal-IO/blob/master/docs/images/logo.png?raw=true)

# The Dal.io Documentation

## Table of Contents

* [Introduction](#introduction)
* [Instalation](#instalation)
* [A Guided Example](#a-guided-example)
* [Next Steps](#next-steps)


## Introduction

Dal-io is a financial modeling package for python aiming to facilitate the gathering, wrangling and analysis of financial data. The library uses **graphical object structures** and **progressive display of complexity** to make workflows suit the user's specific proficiency in python whithout making efficiency
sacrifices. 

The core library implements common workflows from well-supported packages and the means to flexibly interlink them, and aims to continue adding relevent features. However, the user is not constrained by these features, and is free to extend pieces through inheritance in order to implement extra functionality that can be used with the rest of the package. See `developers_guide` for more information on extending core features.


## Instalation

You can clone this repository from git using

```bash
    git clone https://github.com/renatomatz/Dal-io
```

If you are using Windows, make sure you are in the package folder to use the functionality and that you run the following command before importing the modules.

```python
    import sys
    sys.path.append("/path-to-dalio/Dal-io") 
```

For Linux and Mac, you can access the package contents from your python environment anywhere with

```bash
    export PYTHONPATH=$PYTHONPATH:"path/to/Dal-io"
```


## A Guided Example

Let's go through a quick example of what Dal-io can do. We'll build a simple portfolio optimization workflow and test it out with some sample stocks.

This example will be fairly dry, so if you want to jump right into it with some understanding of the Dal-io mechanics, you can go through the `beginners_guide` first. If you just want to see what the library is capable of, let's get right to it.

We'll start off by importing the Dal-io pieces

```python
    import numpy as np

    import dalio.external as de
    import dalio.translator as dt
    import dalio.pipe as dp
    import dalio.model as dm
    import dalio.application as da
```

Specific pieces can also be imported inidvidually, though for testing this sub-module import structure is preferred.

Now lets set up our stock data input from Yahoo! Finance.

```python
    tickers = ["GOOG", "MSFT", "ATVI", "TTWO", "GM", "FORD", "SPY"]
    
    stocks = dt.YahooStockTranslator()\
        .set_input(de.YahooDR())
```

Easy right? Notice that the stock input is composed of one external source (in this case `de.YahooDR`) and one translator (`dt.YahooStockTranslator`). This is the case for any input, with one piece getting raw data from an external source and another one translating it to a format friendly to Dal-io pieces. For more on formatting, go to `formatting`. 

Notice the `.set_input` call that took in the YahooDR object. Every all translators, pipes, models and applications share this method that allows them to plug the ourput of another object as their own input. This idea of connecting different objects like nodes in a graph is at the core of the **graphical object design**.

At this point you can try out running the model with :code:`stocks.run(ticker=tickers)` which will get the OHLCV data for the ticker symbols assigned to :code"`tickers`, though you can specify any ticker available in Yahoo! finance. Notice that the column names where standardized to be all lower-case with undercores (\_) instead of spaces. This is performed as part of the translation step to ensure all imported data can be referenced with common string representations.

Now lets create a data processing pipeline for our input data.

```python
    time_conf = dp.DateSelect()

    close = dp.PipeLine(
        dp.ColSelect(columns="close"),
        time_conf
    )(stocks)

    annual_rets = close + \
        dp.Period("Y", agg_func=lambda x: x[-1]) + \
        dp.Change(strategy="pct_change")

    cov = dp.Custom(lambda df: df.cov(), strategy="pipe")\
        .with_input(annual_rets)

    exp_rets = annual_rets + dp.Custom(np.mean)
```

That was a bit more challenging! Let's take it step by step.

We started off defining a DateSelect pipe (which we will use later) and passingit into a pipeline with other pipes to get a company's annual returns. Pipelines aggregate zero or more Pipe objects and pass in a common input through all of their transformations. This skips data integrity checking while still allowing users to control pipes inside the pipeline from the outside (as we will with `time_conf`)

We then passed added a custom pipe that applies the np.mean function to the annualr returns to get the expected returns for each stock.

Finally, we did the exact same thing but with a lambda that calls the pd.DataFrame internal method .cov() to get the dataframe's covariance. As we will be passing the whole dataframe to the function at once, we set the Custom strategy to "pipe".

Notice how we didn't use `.set_input()` as we did before, that's because we utilized alternative ways of establishing this same node-to-node connection. 

We can connect nodes with:

1. `p1.set_input(p2)` set p1's input to p2.

1. `p1.with_input(p2)` create a copy of p1 and set its input to p2.

1. `p1(p2)` same as `p1.with_input(p2)`.

1. `pL + p2` set p2 as the last transformation in the PipeLine pL.

Now let's set up our efficient frontier model, get the optimal weights and finally create our optimal portfolio model.

```python
    ef = dm.MakeEfficientFrontier(weight_bounds=(-0.5, 1))\
        .set_input("sample_covariance", cov)\
        .set_input("expected_returns", exp_rets)\

    weights = dp.OptimumWeights()(ef)\
        .set_piece("strategy", "max_sharpe", risk_free_rate=0.0)

    opt_port = dm.OptimumPortfolio()\
        .set_input("weights_in", weights)\
        .set_input("data_in", close)
```

And those are two examples of Dal-io Models! As you can see, models can have multiple named inputs, which can be set the same way as you would in a pipe but also having to specify their name. You also saw an example of a Builder, which has pieces (that can be set with the `.set_piece()`) method which allow for more modular flexibility when deciding characteristics of certain pipes or models.We could go into what each source and pieces represents, but that can be better done through the documentation.

Now, as a final step, lets graph the performance of the optimal portfolio.

```python
    graph = da.PandasXYGrapher(x=None, y="close", legend="upper_right")\
        .set_input("data_in", dp.Index(100)(opt_port))\
        .set_output("data_out", de.PyPlotGraph(figsize=(12, 8)))
```

Additionally, you can change the time range of the whole model at any point using the `time_conf` object we created all the way in the beginning. Below is an example of setting the dates from 2016 to 2020.

```python
    time_conf.set_start("2016-01-01")
    time_conf.set_end("2019-12-31")
```

And that's it! 

All that you have to do now is run the model with :code:`graph.run(ticker=tickers)` to 

1. Get stock data from Yahoo! Finance

1. Process data 

1. Optimize portfolio weights

1. Get an optimum portfolio

1. Graph optimum portfolio

Which yields this figure:

![port-opt-graph](https://github.com/renatomatz/Dal-IO/blob/master/docs/images/port_opt_cook_graph.png)

Notice how this :code:`.run()` call was the same as you did all the way back when you only had your imported data. This method is also common to all translators, pipes, models and applications, and it gives you the piece's output. 

This means you can get information of any of the stages you created like this, and for anly stock that you'd like. For example, we can run the :code:`weights` object we created to get the weights associated with the portfolio we just plotted.

```python
    weights.run(ticker=tickers)
```

```bash
    {'GOOG': 0.45514,
     'MSFT': 0.82602,
     'ATVI': -0.49995,
     'TTWO': 0.29241,
     'GM': -0.43788,
     'FORD': 0.38413,
     'SPY': -0.01986}
```

Hope this example was enough to show how you can create clean and powerful models using just a few lines of code!

## Next Steps

If you read and enjoyed the example above, that's great! Now comes the part where you get to understand its various pieces, workflows and internal logic for you to start creating your own models with Dal-io. 

Check out our documentation at [the Dal-io Documentation](https://dalio.readthedocs.io/en/latest/)
