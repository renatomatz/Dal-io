def risk_metrics(df, lamb):
    df = df.copy()**2
    last_ret = df.iloc[0].copy()
    for i in range(1, df.shape[0]):
        curr = df.iloc[i].copy()
        df.iloc[i] = lamb*(df.iloc[i-1]**2) + (1-lamb)*(last_ret)
        last_ret = curr
    return df
