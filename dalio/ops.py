def index_cols(df, i=100):
    return (df / df.iloc[0]) * i


def risk_metrics(df, lamb):
    df = df.copy()**2
    last_ret = df.iloc[0].copy()
    for i in range(1, df.shape[0]):
        curr = df.iloc[i].copy()
        df.iloc[i] = lamb*(df.iloc[i-1]**2) + (1-lamb)*(last_ret)
        last_ret = curr
    return df


def unique(elems):
    """ Thank you, Martin Broadhurst for the beautiful code
    """
    seen = set()
    return [x for x in elems if not (x in seen or seen.add(x))] 
