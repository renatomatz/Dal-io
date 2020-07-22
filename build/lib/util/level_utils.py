"""Utilities for dealing with DataFrame index or column levels"""
from pandas import MultiIndex


def extract_level_names_dict(df):
    """Extract all column names in a dataframe as (level: names_ dicitonar7

    Args:
        df (pd.DataFrame): dataframe whose columns will be extracted
    """
    cols = df.columns

    # create dict of unique elements in each level
    levels = {i: cols.unique(level=i).to_list()
              for i in range(cols.nlevels)}

    return levels


def filter_levels(levels, filters):
    """Filter columns in levels to either be equal to specified
    columns or a filtering function

    Args:
        levels (dict): all column names in a (level: names) dict
        filters (str, list, callable, dict): either columns to place on
            a specified level or filter functions to select columns there.
    """
    if callable(filters):
        return [col for col in levels if filters(col)]
    elif isinstance(filters, dict):
        return {level: filter_levels(levels[level], filt)
                for level, filt in filters.items()}
    elif filters is None:
        return levels

    return filters


def extract_cols(df, cols):
    """Extract columns from a dataframe

    Args:
        df (pd.DataFrame): dataframe containing the columns
        cols (hashable, iterable, dict): single column, list of columnst
            or dict with the level as keys and column(s) as values.

    Raises:
        KeyError: if columns are not in dataframe
    """
    if isinstance(cols, dict):
        sl = get_slice_from_dict(df, cols)
        # probably there's an easier way of doing this
        return df.loc(axis=1).__getitem__(sl)
    elif cols is not None:
        return df[cols]

    return df


def insert_cols(df, new_data, cols):
    """Insert new data into specified existing columns

    Args:
        df (pd.DataFrame): dataframe to insert data into.
        new_data (any): new data to be inserted
        cols (hashable, iterable, dict): existing columns in data.

    Raises:
        KeyError: if columns are not in dataframe
        Exception: if new data doesn't fit cols dimensions
    """
    if isinstance(cols, dict):
        sl = get_slice_from_dict(df, cols)
        # probably there's an easier way of doing this too
        df.loc(axis=1).__setitem__(sl, new_data)
    else:
        df[cols] = new_data

    return df


def drop_cols(df, cols):
    """Drop selected columns from levels

    Args:
        df (pd.DataFrame): dataframe to have columns dropped.
        cols (hashable, iterable, dict): column selection
    """
    if isinstance(cols, dict):
        for level, col in cols.items():
            df = df.drop(columns=col, level=level)
    elif cols is not None:
        df = df.drop(columns=cols)

    return df


def get_slice_from_dict(df, cols):
    """Get a tuple of slices that locate the specified (level: column)
    combination.

    Args:
        df (pd.DataFrame): dataframe with multiindex
        cols (dict): (level: column) dictionary

    Raises:
        ValueError: if any of the level keys are not integers
        KeyError: if any level key is out of bounds or if columns are not in
            the dataframe
    """

    if not all([isinstance(k, int) for k in cols.keys()]):
        raise ValueError("all levels must be integers")
        # TODO: add support for other levels

    if max(cols.keys()) > df.columns.nlevels:
        raise KeyError(f"Specified level is out of bounds, dataframe \
            has {df.columns.nlevels} levels")

    sl = [slice(None)]*df.columns.nlevels
    levels = extract_level_names_dict(df)

    for level, col in cols.items():
        if (
            (isinstance(col, str) and col not in levels[level])
            or
            (isinstance(col, list) and not set(col).issubset(levels[level]))
        ):
            raise KeyError(f"Column {col} not in dataframes columns \
                at level {level}")

        sl[level] = col

    return tuple(sl)


def mi_join(df1, df2, *args, **kwargs):
    """Join two dataframes and sort their columns

    Args:
        df1, df2 (pd.DataFrame): dataframes to join
        *args, **kwargs: arguments for join function (called from df1)

    Raises:
        ValueError if number of levels don't match
    """
    if df1.columns.nlevels != df2.columns.nlevels:
        raise ValueError("Both dataframes must have the same number \
            of levels")

    # join data and sort
    return df1.join(df2, *args, **kwargs).sort_index(axis=1)


def add_suffix(all_cols, cols, suffix):
    """Add suffix to appropriate level in a given column index.

    Args:
        all_cols (pd.Index, pd.MultiIndex): all columns from an index. This
            is only relevent when the columns at hand are a multindex, as each
            tuple element will contain elements from all levels (not only the
            selected ones)
        cols (str, list, dict): selected columns
        suffix (str): the suffix to add to the selected columns.
    """
    if isinstance(cols, str):
        return cols + suffix
    elif isinstance(cols, dict):
        # iterate over level and selected columns
        for level, s_col in cols.items():
            # iterate over current column tuples
            all_cols = all_cols.to_list() if isinstance(all_cols, MultiIndex) \
                else all_cols
            for i, c_col in enumerate(all_cols):
                # we assume c_col is a tuple from a MultiIndex, as col is dict
                lc = list(c_col)
                # if the column name is one of the selected names at a level,
                # add the suffix
                lc[level] = lc[level] + suffix if lc[level] in s_col \
                    else lc[level]
                all_cols[i] = tuple(lc)
        return MultiIndex.from_tuples(all_cols)
    elif hasattr(cols, "__iter__"):
        # Add suffix to every element, assumes all are strings
        return [col + suffix for col in cols]

    return all_cols
