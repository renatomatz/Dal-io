from functools import partial

import numpy as np

from pypfopt import CovarianceShrinkage

from dalio.pipe.app import TransformerApplication

from dalio.validator import HAS_DIMS

from dalio.validator.presets import STOCK_STREAM


class PandasLinearModel(TransformerApplication):
    """Create a linear model from input pandas dataframe, using its index
    as the X value.

    This builder is made up of a single piece: strategy. This piece sets
    which linear model should be used to fit the data.
    """

    def __init__(self):
        """Initialize instance

        Source data is defined to have one or two dimensions.
        """
        super().__init__()

        self._init_piece([
            "strategy"
        ])

        self._source\
            .add_desc(HAS_DIMS(2, comparisson="<="))

    def transform(self, data, **kwargs):
        """Set up fitting parameters and fit built model.

        Returns:
            Fitted linear model
        """
        X = np.arange(len(data.index)).reshape([-1, 1])
        y = data.to_numpy()
        return self.build_model((X, y))

    def build_model(self, data, **kwargs):
        """Build model by returning the chosen model and initialization
        parameters

        Returns:
            Unfitted linear model
        """
        strategy = self._pieces["strategy"]
        return self.interpreter.fit(
            data,
            strategy.name,
            *strategy.args,
            **strategy.kwargs
        )


class CovShrink(TransformerApplication):
    """Perform Covariance Shrinkage on data

    Builder with a single piece: shirnkage. Shrinkage defines what kind of
    shrinkage to apply on a resultant covariance matrix. If none is set,
    covariance will not be shrunk.

    Attributes:
        frequency (int): data time period frequency
    """

    _SHRINKAGE_PRESETS = [
        "shrunk_covariance",
        "ledoit_wolf"
    ]

    frequency: int

    def __init__(self, frequency=252):
        """Initialize instance.

        Defines source data as a stock stream

        Args:
            frequency (int): data time period frequency.

        Raises:
            TypeError: if frequence argument is not an integer.
        """
        super().__init__()

        self._init_piece([
            "shrinkage"
        ])

        self._source\
            .add_desc(STOCK_STREAM)\

        if isinstance(frequency, int):
            self.frequency = frequency
        else:
            raise TypeError(f"argument frequency must be of type {int} not \
                {type(frequency)}")

    def transform(self, data, **kwargs):
        """Build model using data get results.

        Returns:
            A covariance matrix
        """
        return self.build_model(data)()

    def build_model(self, data, **kwargs):
        """Builds Covariance Srhinkage object and returns selected shrinkage
        strategy

        Returns:
            Function fitted on the data.
        """

        cs = CovarianceShrinkage(data, frequency=self.frequency)
        shrink = self._pieces["shrinkage"]

        if shrink.name is None:
            ValueError("shrinkage piece 'name' not set")
        elif shrink.name == "shrunk_covariance":
            shrink_func = cs.shrunk_covariance
        elif shrink.name == "ledoit_wolf":
            shrink_func = cs.ledoit_wolf

        return partial(shrink_func,
                       *shrink.args,
                       **shrink.kwargs)

    def copy(self, *args, **kwargs):
        return super().copy(
            *args,
            frequency=self.frequency,
            **kwargs
        )
