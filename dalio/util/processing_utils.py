import pandas as pd


def process_cols(cols):
    if isinstance(cols, str):
        ret = [cols]
    else:
        ret = cols

    return ret


def process_new_colnames(cols, new_cols):
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
    if cols == new_cols:
        df1 = df1.copy()
        df1.loc(axis=1)[new_cols] = df2
    else:
        df2.columns = new_cols
        df1 = df1.join(df2)
    return df1
