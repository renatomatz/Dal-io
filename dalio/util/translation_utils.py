"""Translation utilities"""


def translate_df(translator, df, inplace=False):
    """Translate dataframe column and index names in accordance to translator
    dictionary.

    Args:
        translator (dict): dictionary of {original: translated} key value
            pairs.
        df (pd.DataFrame): dataframe to have rows and columns translated.
        inplace (bool): whether to perform operation inplace or return a
            translated copy. Optional. Defaults to False.
    """

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
