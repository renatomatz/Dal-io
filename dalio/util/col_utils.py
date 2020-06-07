def process_cols(cols):
    if cols is None:
        ret = []
    elif isinstance(cols, str):
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
        col_names = [new_cols + old_name for old_name in cols]
    
    return col_names
