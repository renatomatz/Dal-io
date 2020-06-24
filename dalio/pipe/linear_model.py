"""Create linear models from input data"""
import numpy as np
from sklearn.linear_model import LinearRegression

from dalio.pipe import Pipe
from dalio.util import _Builder
from dalio.validator import HAS_DIMS


class LinearModel(Pipe, _Builder):
    """Create a linear model from input data.

    This builder is made up of a single piece: strategy. This piece sets
    which linear model should be used to fit the data.
    """

    _STRATEGIES = {
        "LinearRegression": LinearRegression
    }
    # TODO: make this agnostic to upper or lower case

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
        return self.build_model(data).fit(X, y)

    def copy(self, *args, **kwargs):
        return _Builder.copy(self, *args, **kwargs)

    def build_model(self, data):
        """Build model by returning the chosen model and initialization
        parameters

        Returns:
            Unfitted linear model
        """
        strategy = self._piece["strategy"]
        lm = LinearModel._STRATEGIES[strategy["name"]](
            *strategy["args"],
            **strategy["kwargs"]
        )

        return lm
