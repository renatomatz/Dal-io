"""Define statistical models"""

import numpy as np
import pandas as pd

from sklearn.linear_model import LinearRegression

from dalio.base import _Factory
from dalio.model import Model


class XYLinearModel(Model, _Factory):
    """Generalized Linear model for arrays from two sources.

    This Model has two sources, x and y.

    This Builder has one piece. the linear model strategy.
    """

    _STRATEGIES = {
        "LinearRegression": LinearRegression
    }

    def __init__(self):
        """Initialize instance.

        Initialize sources and piece.
        """
        super().__init__()

        self._init_source([
            "x",
            "y",
        ])

        self._init_piece([
            "strategy",
        ])

    def run(self, **kwargs):
        """Get data from both sources, transform them into np.arrays and
        fit the built model
        """
        x = self._source_from("x", **kwargs)
        y = self._source_from("y", **kwargs)

        if isinstance(x, pd.DataFrame):
            x = x.to_numpy()
        elif hasattr(x, "__iter__"):
            x = np.array(x)

        if isinstance(y, pd.DataFrame):
            y = y.to_numpy()
        elif hasattr(y, "__iter__"):
            y = np.array(y)

        return self.build_model((x, y)).fit(x, y)

    def copy(self, *args, **kwargs):
        ret = super().copy(*args, **kwargs)
        ret._pieces = self._pieces
        return ret

    def build_model(self, data, **kwargs):
        """Build model by returning the chosen model and initialization
        parameters

        Returns:
            Unfitted linear model
        """
        strategy = self._pieces["strategy"]
        lm = XYLinearModel._STRATEGIES[strategy.name](
            *strategy.args,
            **strategy.kwargs
        )

        return lm
