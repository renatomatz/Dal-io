from functools import partial

import pandas as pd
import sklearn.preprocessing

from sklearn.preprocessing import StandardScaler

from dalio.pipe import Pipe

from dalio.pipe.extra_classes import _ColSelection

from dalio.util import (
        out_of_place_col_insert,
        process_cols,
        _Builder
)


class Encode(_ColSelection):
    """A pipeline stage that encodes categorical columns to integer values.

    The encoder for each column is saved in the attribute 'encoders', which
    is a dict mapping each encoded column name to the
    sklearn.preprocessing.LabelEncoder object used to encode it.

    Parameters
    ----------
    columns : str or list-like, default None
        Column names in the DataFrame to be encoded. If columns is None then
        all the columns with object or category dtype will be encoded, except
        those given in the exclude_columns parameter.
    exclude_columns : str or list-like, default None
        Name or names of categorical columns to be excluded from encoding
        when the columns parameter is not given. If None no column is excluded.
        Ignored if the columns parameter is given.
    drop : bool, default True
        If set to True, the source columns are dropped after being encoded,
        and the resulting encoded columns retain the names of the source
        columns. Otherwise, encoded columns gain the suffix '_enc'.

    Example
    -------
        >>> import pandas as pd; import pdpipe as pdp;
        >>> data = [[3.2, "acd"], [7.2, "alk"], [12.1, "alk"]]
        >>> df = pd.DataFrame(data, [1,2,3], ["ph","lbl"])
        >>> encode_stage = pdp.Encode("lbl")
        >>> encode_stage(df)
             ph  lbl
        1   3.2    0
        2   7.2    1
        3  12.1    1
        >>> encode_stage.encoders["lbl"].inverse_transform([0,1,1])
        array(['acd', 'alk', 'alk'], dtype=object)
    """

    def __init__(
            self,
            columns=None,
            exclude_columns=None,
            drop=True,
    ):
        super().__init__(columns)

        self._exclude_columns = process_cols(exclude_columns) \
            if exclude_columns is not None else []

        self._drop = drop

    def transform(self, data, **kwargs):

        columns_to_encode = self._cols

        if self._cols is None:
            columns_to_encode = list(
                set(
                    data.select_dtypes(include=["object", "category"]).columns
                ).difference(self._exclude_columns)
            )

        inter_df = data

        for colname in columns_to_encode:

            lbl_enc = sklearn.preprocessing.LabelEncoder()

            source_col = data[colname]
            loc = data.columns.get_loc(colname) + 1
            new_name = colname + "_enc"

            if self._drop:
                inter_df = inter_df.drop(colname, axis=1)
                new_name = colname
                loc -= 1

            inter_df = out_of_place_col_insert(
                df=inter_df,
                series=lbl_enc.fit_transform(source_col),
                loc=loc,
                column_name=new_name,
            )

        return inter_df


class Scale(Pipe, _Builder):
    """A pipeline stage that scales data.

    Parameters
    ----------
    scaler : str
        The type of scaler to use to scale the data. One of 'StandardScaler',
        'MinMaxScaler', 'MaxAbsScaler', 'RobustScaler', 'QuantileTransformer'
        and 'Normalizer'.
    exclude_columns : str or list-like, default None
        Name or names of columns to be excluded from scaling. Excluded columns
        are appended to the end of the resulting dataframe.
    exclude_object_columns : bool, default True
        If set to True, all columns of dtype object are added to the list of
        columns excluded from scaling.
    **kwargs : extra keyword arguments
        All valid extra keyword arguments are forwarded to the scaler
        constructor on scaler creation (e.g. 'n_quantiles' for
        QuantileTransformer). PdPipelineStage valid keyword arguments are used
        to override Scale class defaults.

    Example
    -------
        >>> import pandas as pd; import pdpipe as pdp;
        >>> data = [[3.2, 0.3], [7.2, 0.35], [12.1, 0.29]]
        >>> df = pd.DataFrame(data, [1,2,3], ["ph","gt"])
        >>> scale_stage = pdp.Scale("StandardScaler")
        >>> scale_stage(df)
                 ph        gt
        1 -1.181449 -0.508001
        2 -0.082427  1.397001
        3  1.263876 -0.889001
    """

    _SCALER_DICT = {
        "StandardScaler": StandardScaler
    }

    def __init__(
            self,
            exclude_columns=None,
            exclude_object_columns=True,
    ):
        super().__init__()

        self._init_piece([
            "scaler"
        ])

        self._exclude_columns = process_cols(exclude_columns) \
            if exclude_columns is not None else []

        self._exclude_obj_cols = exclude_object_columns

    def transform(self, data, **kwargs):

        cols_to_exclude = self._exclude_columns.copy()

        if self._exclude_obj_cols:
            obj_cols = list((data.dtypes[data.dtypes == object]).index)
            obj_cols = [x for x in obj_cols if x not in cols_to_exclude]
            cols_to_exclude += obj_cols

        if cols_to_exclude:
            excluded = data[cols_to_exclude]
            apply_to = data[
                [col for col in data.columns if col not in cols_to_exclude]
            ]
        else:
            apply_to = data

        try:
            res = pd.DataFrame(
                data=self.build_model(data).fit_transform(apply_to),
                index=apply_to.index,
                columns=apply_to.columns,
            )
        except Exception:
            raise RuntimeError(
                "Exception raised when Scale applied to columns {}".format(
                    apply_to.columns
                )
            )

        if cols_to_exclude:
            res = pd.concat([res, excluded], axis=1)
            res = res[data.columns.to_list()]

        return res

    def build_model(self, data):
        scaler = self._piece["scaler"]
        return partial(
            Scale._SCALER_DICT[scaler["name"]],
            *scaler["args"],
            **scaler["kwargs"]
        )

    def check_name(self, param, name):
        super().check_name(param, name)
        if name not in Scale._SCALER_DICT:
            raise ValueError(f"argument shrinkage must be one of \
                {Scale._SCALER_DICT}")
