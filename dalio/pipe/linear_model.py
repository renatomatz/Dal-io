import numpy as np
from sklearn.linear_model import LinearRegression

from dalio.pipe import Pipe
from dalio.util import _Builder
from dalio.validator import HAS_DIMS


class TSLinearModel(Pipe, _Builder):

    _STRATEGIES = {
        "LinearRegression": LinearRegression
    }
    # TODO: make this agnostic to upper or lower case

    def __init__(self, model=None):
        super().__init__()

        self._init_piece([
            "strategy"
        ])

        self._source\
            .add_desc(HAS_DIMS(1))

    def transform(self, data, **kwargs):
        X = np.arange(len(data.index)).reshape([-1, 1])
        y = data.to_numpy()
        return self.build_model(data).fit(X, y)

    def copy(self):
        ret = type(self)()

        # users shouldn't have access to arg and kwarg keys from pieces
        # attribute, so no need to deep copy those too
        ret._piece = self._piece.copy()
        return ret

    def build_model(self, data):

        # set mean model piece
        strategy = self._piece["strategy"]
        lm = TSLinearModel._STRATEGIES[strategy["name"]](
            *strategy["args"],
            **strategy["kwargs"]
        )

        return lm
