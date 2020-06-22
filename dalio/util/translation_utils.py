def translate_df(translator, df, inplace=False):
    '''Translate dataframe column and index names in accordance to translator
    '''

    if not inplace:
        df = df.copy()

    # translate index and column names
    df.index.set_names(
        translator.translate_item(df.index.names), 
        inplace=True)
    df.columns.set_names(
        translator.translate_item(df.columns.names),
        inplace=True)

    # translate column
    df.rename(
        mapper=translator.translations,
        axis=1,
        inplace=True)

    return df


def get_numeric_column_names(df):
    """Return the names of all columns of numeric type.

    Parameters
    ----------
    df : pandas.DataFrame
        The dataframe to get numeric column names for.

    Returns
    -------
    list of str
        The names of all columns of numeric type.

    Example
    -------
        >>> import pandas as pd; import pdpipe as pdp;
        >>> data = [[2, 3.2, "acd"], [1, 7.2, "alk"], [8, 12.1, "alk"]]
        >>> df = pd.DataFrame(data, [1,2,3], ["rank", "ph","lbl"])
        >>> sorted(get_numeric_column_names(df))
        ['ph', 'rank']
    """
    num_cols = []
    for colbl, dtype in df.dtypes.to_dict().items():
        if np.issubdtype(dtype, np.number):
            num_cols.append(colbl)
    return num_cols
