from itertools import product

from dalio.model.app import TransformerApplication


class MultiGrapher(TransformerApplication):
    """Grapher for multiple inputs taking in the same keyword arguments.

    This is useful to greate subplots of the same data processed in
    different ways. Sources are the data inputs and pieces are their kinds,
    args and kwargs.

    This applicaiton can N sources and pieces, where N is the total number of
    graphs.
    """

    def __init__(self, rows, cols):
        """Initialize instance.

        This generates one source and piece per figure (product of rows and
        cols) and one output.

        Args:
            rows (int): number of rows (starting at one).
            cols (int): number of cols (starting at one).
        """

        super().__init__()

        self._init_source(
            product(
                range(rows),
                range(cols)
            ),
        )

        self._init_output([
            "data_out",
        ])

        self._init_piece(
            product(
                range(rows),
                range(cols)
            ),
        )

    def run(self, **kwargs):
        """Gets data input from each source and plots it using the set
        information in each piece
        """
        for coord in self._source:

            data = self._source_from(coord, **kwargs)

            if data is None:
                continue

            data, kind, f_kwargs = self.build_model(data, coord=coord)

            graph_opts = kwargs.get("graph_opts", {})
            graph_opts.update(f_kwargs)

            self._get_output("data_out")\
                .plot(data, coord, kind=kind, **graph_opts)

        return self._get_output("data_out").request()

    def build_model(self, data, **kwargs):
        """Return data unprocessed"""
        plot = self._pieces[kwargs.get("coord", None)]
        return data, plot.name, plot.kwargs


class PandasMultiGrapher(MultiGrapher):
    """Multigrapher with column selection mechanisms

    In this MultiGrapher, you can select any x, y and z columns as piece
    kwargs and they will be interpreted during the run. Keep in mind that
    this allows for any combination of these layered one on top of each other
    regardless of name. If you specify an "x" and a "z", the "z" column will
    be treated like a "y" column.

    There are also no interpretations of what
    is to be graphed, and thus all wanted columns should be specified.

    There is one case for indexes, where the x_index, y_index or z_index
    keyword arguments can be set to True.
    """

    def build_model(self, data, **kwargs):
        """Process data columns"""

        data, kind, f_kwargs = super().build_model(data, **kwargs)
        cols = []

        for ax in "xyz":
            ax, ax_index = \
                f_kwargs.pop(ax, None), \
                f_kwargs.pop(ax+"_index", False)

            if ax is None:
                if ax_index:
                    cols.append(data.index)
            else:
                cols.append(data.loc(axis=1)[ax])

        return tuple(cols), kind, f_kwargs
