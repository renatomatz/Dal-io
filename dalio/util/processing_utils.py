import pandas as pd


def process_cols(columns):
    if isinstance(columns, str):
        return [columns]
    elif hasattr(columns, '__iter__'):
        return columns
    return columns


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


def list_str(listi):
    if listi is None:
        return None
    if isinstance(listi, (list, tuple)):
        return ', '.join([str(elem) for elem in listi])
    return listi


def _filter_cols(all_cols, cols):
    if callable(cols):
        filtered_cols = [col for col in all_cols if cols(col)]
    elif cols is None:
        filtered_cols = all_cols
    else:
        filtered_cols = cols

    return filtered_cols
