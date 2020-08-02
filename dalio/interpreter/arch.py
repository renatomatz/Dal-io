"""Instanciate interpreters for ARCH statistical modeling"""

from arch.univariate.base import ARCHModel

# Import mean models
from arch.univariate import (
    ConstantMean,
    ARX
)

# Import variance models
from arch.univariate import (
    VolatilityProcess,
    ARCH,
    GARCH,
    EWMAVariance
)

# Import distribution models
from arch.univariate import (
    Distribution,
    Normal,
    StudentsT,
    SkewStudent
)

from dalio.interpreter import _Interpreter


class _ARCH(_Interpreter):
    """Interpreter class for a generalized ARCH model

    This lays out the basic features and mechanics of an ARCH model and
    its subclasses can implement them to fit a specific package.

    Attributes:
        _engine: model functionality engine
        _mean: mean model to be used on ARCH fitting
        _volatility: volatility model
        _distribution: data distribution function
    """

    def __init__(self):
        super().__init__()

        self.clear()

    @property
    def mean(self):
        """Get the mean estimation model"""
        raise NotImplementedError()

    @mean.setter
    def mean(self, mean):
        """Set the mean estimation model"""
        raise NotImplementedError()

    @property
    def volatility(self):
        """Get the volatility estimation model"""
        raise NotImplementedError()

    @volatility.setter
    def volatility(self, volatility):
        """Set the volatility estimation model"""
        raise NotImplementedError()

    @property
    def distribution(self):
        """Get the dependent variable distribution"""
        raise NotImplementedError()

    @distribution.setter
    def distribution(self, distribution):
        """Set the dependent variable distribution"""
        raise NotImplementedError()

    def fit(self, data, *args, **kwargs):
        """Fit model to data"""
        raise NotImplementedError()

    def forecast(self, *args, n_steps=1, **kwargs):
        """Use fitted model to make a forecast. This is a less standardized
        version of its more specific counterparts and should be used for
        user analysis only. Use more objective forecasts when implementing
        a graph piece.

        Args:
            n_steps (int): number of steps to forecast.
        """
        raise NotImplementedError()

    def forecast_mean(self, *args, n_steps=1, **kwargs):
        """Use fitted model to make a forecast of future mean.

        Args:
            n_steps (int): number of steps to forecast.
        """
        raise NotImplementedError()

    def forecast_variance(self, *args, n_steps=1, **kwargs):
        """Use fitted model to make a forecast of future variance.

        Args:
            n_steps (int): number of steps to forecast.
        """
        raise NotImplementedError()

    def get_fit_params(self):
        """Get the estimated and set model parameters"""
        raise NotImplementedError()

    def get_dist_ppf(self):
        """Get the inverse cumulative density function from fitted
        distribution
        """
        raise NotImplementedError()

    def get_orig_data(self):
        """Get data used for fitting model"""
        raise NotImplementedError()

    def get_fit_indices(self):
        """Get the indices of the fit data, which might vary depending on
        the algorithm or parameters
        """
        raise NotImplementedError()


class ARCHInterpreter(_ARCH):
    """Interpreter for the arch package.

    Implements arch model mechanics

    Attributes:
        _fit_res (ARCHModelResult): result from fitting model. This is
            specific to the arch package and must be kept separate from the
            engine, as this variable is what contains most of the parameters.
            This is reset at every change of model parameters, as the fit
            results should only reflect the latest model.
    """

    _MEAN_DICT = {
        "ConstantMean": ConstantMean,
        "constant": ConstantMean,
        "ARX": ARX,
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
        "StudentsT": StudentsT,
        "SkewStudent": SkewStudent
    }

    def clear(self):
        super().clear()
        # set all model parameters to None.
        self._engine \
            = self._mean \
            = self._volatility \
            = self._distribution \
            = self._fit_res \
            = None

    @property
    def engine(self):
        if self._engine is not None:
            return self._engine

        raise AttributeError("ARCH engine is not set")

    @engine.setter
    def engine(self, engine):
        if isinstance(engine, ARCHModel):
            # if new engine is being set, set it and make
            self._engine = self._mean = engine
            self.engine.volatility = self.volatility
            self.engine.distribution = self.distribution
        elif engine is None:
            self.clear()

        raise TypeError(f"engine must be of type {ARCHModel} not \
            {type(engine)}")

    @property
    def mean(self):
        return self._mean

    @mean.setter
    def mean(self, mean):
        try:
            # the mean model of the arch package implements all garch
            # functionalities, and thus must be handled like the engine
            self.engine = mean
        except TypeError:
            # translate TypeError so that theres no confusion from the part
            # of the user.
            raise TypeError(f"mean must be of type {ARCHModel} not \
                {type(mean)}")

    def init_mean(self, mean, *args, data=None, **kwargs):
        """Initialize a new mean object from given data.

        Args:
            data (array-like): data to fit the mean on.
            mean (str): mean name.
            *args, **kwargs: mean initialization parameters.
        """
        if callable(mean):
            self.mean = mean(
                data,
                *args,
                **kwargs
            )
        else:
            self.mean = ARCHInterpreter._MEAN_DICT[mean](
                data,
                *args,
                **kwargs
            )

    @property
    def volatility(self):
        return self._volatility

    @volatility.setter
    def volatility(self, volatility):
        if isinstance(volatility, VolatilityProcess):
            self.engine.volatility = self._volatility = volatility
            self._fit_res = None

        raise TypeError(f"volatility must be of type {VolatilityProcess} not \
            {type(volatility)}")

    def init_volatility(self, volatility, *args, data=None, **kwargs):
        """Initialize a new volatility object from given data.

        Args:
            data (array-like): data to fit the volatility on.
            volatility (str): volatility name.
            *args, **kwargs: volatility initialization parameters.
        """
        if callable(volatility):
            self.volatility = volatility(
                data,
                *args,
                **kwargs
            )
        else:
            self.volatility = ARCHInterpreter._VOL_DICT[volatility](
                data,
                *args,
                **kwargs
            )

    @property
    def distribution(self):
        return self._distribution

    @distribution.setter
    def distribution(self, distribution):
        if isinstance(distribution, Distribution):
            self.engine.distribution = self._distribution = distribution
            self._fit_res = None

        raise TypeError(f"distribution must be of type {Distribution} not \
            {type(distribution)}")

    def init_distribution(self, distribution, *args, data=None, **kwargs):
        """Initialize a new distribution object from given data.

        Args:
            data (array-like): data to fit the distribution on.
            distribution (str): distribution name.
            *args, **kwargs: distribution initialization parameters.
        """
        if callable(distribution):
            self.distribution = distribution(
                data,
                *args,
                **kwargs
            )
        else:
            self.distribution = ARCHInterpreter._DIST_DICT[distribution](
                data,
                *args,
                **kwargs
            )

    def fit(self, data, *args, **kwargs):
        self._fit_res = self.engine.fit(*args, **kwargs)

    def forecast(self, *args, n_steps=1, **kwargs):
        return self.engine.forecast(*args, horizon=n_steps, **kwargs)

    def forecast_mean(self, *args, n_steps=1, **kwargs):
        return self.forecast(*args, horizon=n_steps, **kwargs).mean

    def forecast_variance(self, *args, n_steps=1, **kwargs):
        return self.forecast(*args, horizon=n_steps, **kwargs).mean

    def get_fit_params(self):

        if self._fit_res._fit_params is None:
            raise AttributeError("Model is not fit")

        dist = type(self.distribution).__name__

        if dist == "Normal":
            return None
        elif dist == "StudentsT":
            return self._fit_res.get_fit_params()[-1]
        elif dist == "SkewStudent":
            return self._fit_res.get_fit_params()[-2:]

        raise TypeError(f"model has unsuported distribution {dist}")

    def get_fit_indices(self):
        return self._fit_res._fit_indices

    def get_dist_ppf(self):
        return self.distribution.ppf

    def get_orig_data(self):
        return self.engine._y_original
