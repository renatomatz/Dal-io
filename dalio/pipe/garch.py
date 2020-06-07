from arch.univariate.base import ARCHModel

# Import mean models
from arch.univariate import (
    ConstantMean,
    ARX
)

# Import variance models
from arch.univariate import (
    ARCH,
    GARCH,
    EWMAVariance
)

# Import distribution models
from arch.univariate import (
    Normal,
    StudentsT
)

from dalio.util import _Builder
from dalio.pipe import Pipe
from dalio.validator import HAS_DIMS


class MakeARCH(Pipe, _Builder):

    _MEAN_DICT = {
        "ConstantMean": ConstantMean,
        "constant": ConstantMean,
        "ARX": ARX,
        "arx": ARX
    }
    _VOL_DICT = {
        "ARCH": ARCH,
        "GARCH": GARCH,
        "EWMAVariance": EWMAVariance,
        "EWMA": EWMAVariance,
        "RiskMetrics": EWMAVariance
    }
    _DIST_DICT = {
        "Normal": Normal,
        "normal": Normal,
        "StudentsT": StudentsT
    }
    # TODO: make this agnostic to upper or lower case

    def __init__(self, model=None):
        super().__init__()

        self._init_piece([
            "mean",
            "volatility",
            "distribution"
        ])

        self._source\
            .add_desc(HAS_DIMS(1))

        if isinstance(model, ARCHModel):
            self.assimilate(model)

    def transform(self, data, **kwargs):
        return self.build_model(data).fit(**kwargs.get("fit_opts", {}))

    def copy(self):
        ret = type(self)()

        # users shouldn't have access to arg and kwarg keys from pieces
        # attribute, so no need to deep copy those too
        ret._piece = self._piece.copy()
        return ret

    def build_model(self, data):

        # set mean model piece
        mean = self._piece["mean"]
        am = MakeARCH._MEAN_DICT[mean["name"]](
            data,
            *mean["args"],
            **mean["kwargs"]
        )

        # set volatility model piece
        vol = self._piece["volatility"]
        am.volatility = MakeARCH._VOL_DICT[vol["name"]](
            *vol["args"],
            **vol["kwargs"]
        )

        # set distribution model piece
        dist = self._piece["distribution"]
        am.distribution = MakeARCH._DIST_DICT[dist["name"]](
            *dist["args"],
            **dist["kwargs"]
        )

        return am

    def assimilate(self, model):
        # shallow assimilation

        self.set_piece(
            "mean",
            type(model).__name__
        )
        self.set_piece(
            "volatility",
            type(model.volatility).__name__
        )
        self.set_piece(
            "distribution",
            type(model.distribution).__name__
        )
