import matplotlib.pyplot as plt

from dalio.external import External


class PyPlotGraph(External):

    _connection: plt.Figure

    def __init__(self):
        super().__init__()
        self._connection = plt.figure()

    def request(self, **kwargs):
        return self._connection

    def plot(self, data, **graph_opts):
        self._connection = plt.figure()
        plt.plot(data, **graph_opts)


class PySubplotGraph(PyPlotGraph):

    _axes: dict()

    def __init__(self, rows, cols):
        pass
        # TODO: implement other methods
