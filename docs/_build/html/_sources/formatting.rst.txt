.. _formatting:

Formatting
==========

Formatting is key to making input from various sources accessible throughout the graph for standard analyses. One main advantage of Dal-io is the fact that inputs can be switched easily, the cost of this comes at having to compromise on formats to be used throughout the graph.

Constants
---------

Constants are defined in the :code:`dalio.base.constants` script and are used in the library instead of string names. This is done so to maintain data integrity throughout the graph, such that there is no confusion amongst pieces as to what should they be naming something or what name they should check for in case they require something. 

If you want to create a piece dependent on inputs having a certain name characteristic (containing a specific column for example), the checks should be done on the name constant only. Likewise, if you are creating a piece that generates an input matching that characteristic, you should also use the constants to name it. 

**This is not applicable to any names users must specify. These should be set as strings.**

If you do not find the name you are looking for in the constants, add it to the script in alphabetical order.

Time Series
-----------

A time series is a :code:`pandas.DataFrame` or :code:`pandas.Series` which has a :code:`pandas.DatetimeIndex` as its index with unique timestamps and a single level.

Any other information must be stored in the columns such that the index remains unique and at a single level. This means that any sort of category that repeats the date should be set as a column level.

KEEP IN MIND that just because a dataset contains date columns, doesn't make it a time series. If you notice that making a dataframe be of the above format yields a very sparse dataset with few, oddly-spaced time steps and an unmanageable amount of columns, it is unlikely a time series (as per our definitions).
