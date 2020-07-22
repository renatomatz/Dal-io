"""Plotting utilities

Thank you for the creators of pypfopt for the wonderful code!
"""

import numpy as np
from pypfopt import risk_models
import scipy.cluster.hierarchy as sch

import matplotlib.pyplot as plt

plt.style.use("seaborn-deep")


def plot_covariance(
        cov_matrix,
        plot_correlation=False,
        show_tickers=True,
        ax=None):
    """Generate a basic plot of the covariance (or correlation) matrix,
    given a covariance matrix.

    Args:
        cov_matrix (pd.DataFrame, np.ndarray): covariance matrix
        plot_correlation (bool): whether to plot the correlation matrix
            instead, defaults to False. Optional.
        show_tickers (bool): whether to use tickers as labels (not
            recommended for large portfolios). Optional. Defaults to True.
        ax (matplolib.axis, None): Axis to plot on. Optional. New axis will
            be created if none is specified.

    Returns:
        matplotlib axis
    """
    if plot_correlation:
        matrix = risk_models.cov_to_corr(cov_matrix)
    else:
        matrix = cov_matrix

    if ax is None:
        _, ax = plt.subplots()

    if show_tickers:
        ax.set_xticks(np.arange(0, matrix.shape[0], 1))
        ax.set_xticklabels(matrix.index)
        ax.set_yticks(np.arange(0, matrix.shape[0], 1))
        ax.set_yticklabels(matrix.index)
        plt.xticks(rotation=90)

    return ax


def plot_dendrogram(hrp, show_tickers=True, ax=None, **kwargs):
    """Plot the clusters in the form of a dendrogram.

    Args:
        hrp: HRPpt object that has already been optimized.
        show_tickers (bool): whether to use tickers as labels (not
            recommended for large portfolios). Optional. Defaults to True.
        ax (matplolib.axis, None): Axis to plot on. Optional. New axis will
            be created if none is specified.
        **kwargs: optional parameters for main graph.

    Returns:
        matplotlib axis
    """
    if hrp.clusters is None:
        hrp.optimize()

    if ax is None:
        _, ax = plt.subplots()

    if show_tickers:
        sch.dendrogram(
            hrp.clusters,
            labels=hrp.tickers,
            ax=ax,
            orientation="top",
            **kwargs
        )
        plt.xticks(rotation=90)
        plt.tight_layout()
    else:
        sch.dendrogram(hrp.clusters, no_labels=True, ax=ax)

    return ax


def plot_efficient_frontier(
        cla,
        points=100,
        visible=25,
        show_assets=True,
        ax=None,
        **kwargs):
    """Plot the efficient frontier based on a CLA object

    Args:
        points (int): number of points to plot. Optional. Defaults to 100
        show_assets (bool): whether we should plot the asset risks/returns
            also. Optional. Defaults to True.
        ax (matplolib.axis, None): Axis to plot on. Optional. New axis will
            be created if none is specified.
        **kwargs: optional parameters for main graph.

    Returns:
        matplotlib axis
    """
    if cla.weights is None:
        cla.max_sharpe()
    optimal_ret, optimal_risk, _ = cla.portfolio_performance()

    if cla.frontier_values is None:
        cla.efficient_frontier(points=points)

    mus, sigmas, _ = cla.frontier_values

    if ax is None:
        _, ax = plt.subplots()

    ax.plot(sigmas, mus, label="Efficient frontier", **kwargs)

    sl = slice(0, None, points//visible)
    ax.scatter(sigmas[sl], mus[sl], **kwargs)

    if show_assets:
        zipped = zip(
            np.sqrt(np.diag(cla.cov_matrix)),
            cla.expected_returns,
            np.array(cla.tickers)
        )
        for x, y, s in zipped:
            ax.text(x, y, s, fontsize=10)

    ax.scatter(
        optimal_risk,
        optimal_ret,
        marker="x",
        s=100,
        color="r",
        label="optimal"
    )
    ax.legend()
    ax.set_xlabel("Volatility")
    ax.set_ylabel("Return")

    return ax


def plot_weights(weights, ax=None, **kwargs):
    """Plot the portfolio weights as a horizontal bar chart

    Args:
        weights (dict): the weights outputted by any PyPortfolioOpt
            optimiser.
        ax (matplolib.axis, None): Axis to plot on. Optional. New axis will
            be created if none is specified.
        **kwargs: optional parameters for main graph.

    Returns:
        matplotlib axis
    """
    desc = sorted(weights.items(), key=lambda x: x[1], reverse=True)
    labels = [i[0] for i in desc]
    vals = [i[1] for i in desc]

    y_pos = np.arange(len(labels))

    if ax is None:
        _, ax = plt.subplots()

    ax.barh(y_pos, vals, **kwargs)
    ax.set_xlabel("Weight")
    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels)
    ax.invert_yaxis()

    return ax
