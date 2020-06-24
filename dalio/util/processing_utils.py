"""Data processing utilities"""
import pandas as pd


def process_cols(cols):
    """Standardize input columns"""
    if isinstance(cols, str):
        ret = [cols]
    else:
        ret = cols

    return ret


def process_new_colnames(cols, new_cols):
    """Get new column names based on the column parameter"""
    col_names = None
    if new_cols is None:
        col_names = cols
    elif isinstance(new_cols, list):
        col_names = cols
    elif isinstance(new_cols, str):
        if isinstance(cols[0], tuple):
            # append new column name to last element of tuple
            col_names = [
                old_name[:-1] + (new_cols + "_" + old_name[-1],)
                for old_name in cols]
        else:
            col_names = [new_cols + old_name for old_name in cols]

    return col_names


def process_date(date):
    """Standardize input date

    Raises:
        TypeError: if the type of the date parameter cannot be converted to
            a pandas timestamp
    """
    if date is None or isinstance(date, pd.Timestamp):
        return date
    else:
        try:
            return pd.Timestamp(date)
        except TypeError:
            raise TypeError(f"value type {type(date)} not convertible \
                    to pandas timestamp")
        except ValueError:
            raise ValueError(f"value {date} not convertible to \
                    to pandas timestamp")


def process_new_df(df1, df2, cols, new_cols):
    """Process new dataframe given columns and new column names

    Args:
        df1 (pd.DataFrame): first dataframe.
        df2 (pd.DataFrame): dataframe to join or get columns from
        cols (iterable): iterable of columns being targetted.
        new_cols (iterable): iterable of new column names.
    """
    if cols == new_cols:
        df1 = df1.copy()
        df1.loc(axis=1)[new_cols] = df2
    else:
        df2.columns = new_cols
        df1 = df1.join(df2)
    return df1
