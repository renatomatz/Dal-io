def index_cols(df, i=100):
    return (df / df.iloc[0]) * i
