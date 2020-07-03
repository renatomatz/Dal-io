"""Define basic models"""


from dalio.model import Model


class Join(Model):
    """Join two dataframes on index.

    This model has two sources: left and right.

    Attributes:
        _kwargs (dict): optional keyword arguments for pd.join
    """

    def __init__(self, **kwargs):
        super().__init__()

        self._init_source([
            "left",
            "right",
        ])

        self._kwargs = kwargs

    def run(self, **kwargs):
        """Get left and right side data and join"""
        left = self._source_from("left", **kwargs)
        right = self._source_from("right", **kwargs)
        return left.join(right, **self._kwargs)
