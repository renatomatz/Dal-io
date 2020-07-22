"""Implement linear model interpreters"""

import numpy as np

from sklearn.linear_model._base import LinearModel
from sklearn.linear_model import LinearRegression

from statsmodel.regression.linear_model import RegressionModel
from statsmodel import add_constant, OLS

from dalio.base import _Interpreter


class _LinearModel(_Interpreter):
    """Interpreter for generalized linear statistical models.

    This lays out the basic feature and mechanisms of linear and common
    metrics associated with a fitted model. Its subclasses can implement them
    to fit a specific package.
    """

    def __init__(self):
        super().__init__()

        self.clear()

    def init_engine(self, strategy, *args, **kwargs):
        """Initialize linear model engine with required args and kwargs."""
        raise NotImplementedError()

    def fit(self, data, strategy, *args, **kwargs):
        """Fit the regression model to the data"""
        raise NotImplementedError()

    def predict(self, data, *args, **kwargs):
        """Predict values based on regression model"""
        raise NotImplementedError()

    def get_fitted_params(self, *args, **kwargs):
        """Get all parameters arrived at durring fitting process"""
        raise NotImplementedError()

    @property
    def coef(self):
        """Get fitted coefficients"""
        raise NotImplementedError()

    @property
    def intercept(self):
        """Get fitted intercepts"""
        raise NotImplementedError()

    @property
    def r_squared(self):
        """Get model R^2 score evaludated on fitted data"""
        raise NotImplementedError()


class SklearnInterpreter(_LinearModel):
    """Implmement interpreter for linear model workflows in the sklearn
    package

    Attributes:
        _r_squared (float): R^2 score. Set durring fitting.
    """

    _STRATEGIES = {
        "LinearRegression": LinearRegression
    }

    def __init__(self):
        super().__init__()

        self._r_squared = None
        self.clear()

    def clear(self):
        self._engine = \
            self._r_squared = \
            None

    @property
    def engine(self):
        if self._engine is not None:
            return self._engine

        raise AttributeError("linear model is not set")

    @engine.setter
    def engine(self, engine):
        if isinstance(engine, LinearModel):
            self._engine = engine

        raise TypeError(f"engine parameter must be of type \
            {LinearModel}, not {type(engine)}")

    def init_engine(self, data, strategy, *args, **kwargs):
        self.engine = SklearnInterpreter._STRATEGIES[strategy](
            *args,
            **kwargs,
        )

    def fit(self, data, *args, **kwargs):

        self.engine.fit(*data, *args, **kwargs)

        self._r_squared = self.engine.score(*data)

    def predict(self, data, *args, **kwargs):
        """Predict values based on regression model"""
        self.engine.predict(data)

    def get_fitted_params(self, *args, **kwargs):
        """Get all parameters arrived at durring fitting process"""
        return np.array([self.coef, self.intercept])

    @property
    def coef(self):
        """Get fitted coefficients"""
        return self.engine.coef_

    @property
    def intercept(self):
        """Get fitted intercepts"""
        return self.engine.intercept_

    @property
    def r_squared(self):
        """Get model R^2 score evaludated on fitted data"""
        return self._r_squared


class StatsModelInterpreter(_LinearModel):
    """Implmement interpreter for linear model workflows in the statsmodel
    package.

    Attributes:
        _fit_res (RegressionResults) results from fitting model.
    """

    _STRATEGIES = {
        "LinearRegression": OLS,
        "OLS": OLS,
    }

    def __init__(self):
        super().__init__()

        self._fit_res = None
        self.clear()

    def clear(self):
        self._engine = \
            self._fit_res = \
            None

    @property
    def engine(self):
        if self._engine is not None:
            return self._engine

        raise AttributeError("linear model is not set")

    @engine.setter
    def engine(self, engine):
        if isinstance(engine, RegressionModel):
            self._engine = engine

        raise TypeError(f"engine parameter must be of type \
            {RegressionModel}, not {type(engine)}")

    def init_engine(self, data, strategy, *args, **kwargs):

        if isinstance(data, tuple) and len(data) == 2:
            endog, exog = data
        else:
            endog, exog = data, None

        endog = add_constant(endog)

        self.engine = StatsModelInterpreter._STRATEGIES[strategy](
            endog, exog=exog,
            *args,
            **kwargs,
        )

        self._fit_res = None

    def fit(self, data, *args, **kwargs):

        self._fit_res = self.engine.fit(*args, **kwargs)

    def predict(self, data, *args, **kwargs):
        """Predict values based on regression model"""
        self.engine.predict(data)

    def get_fitted_params(self, *args, **kwargs):
        """Get all parameters arrived at durring fitting process"""
        return self._fit_res.params

    @property
    def coef(self):
        """Get fitted coefficients"""
        return self._fit_res.params[0]

    @property
    def intercept(self):
        """Get fitted intercepts"""
        return self._fit_res.params[1]

    @property
    def r_squared(self):
        """Get model R^2 score evaludated on fitted data"""
        return self._fit_res.rsquared
