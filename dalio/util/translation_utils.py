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
