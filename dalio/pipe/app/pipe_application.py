from dalio.base import _Factory
from dalio.pipe import Pipe


class PipeApplication(Pipe, _Factory):
    """Hybrid factory type for complementing Pipe instances.

    These specify extra methods implemented by Pipe instances to better 
    inherit from _Factory instances. Most of what is done here is to allow
    the development of PipeApplication instances to have the exact same
    workflow as developping a simple Pipe instance.
    """

    def with_piece(self, param, name, *args, **kwargs):
        """Copy self and return with a new piece set"""
        return _Factory.set_piece(
            self.copy(),
            param,
            name,
            *args, **kwargs
        )

    def copy(self, *args, **kwargs):
        ret = super().copy(*args, **kwargs)
        return _Factory.update_pieces(ret, self._pieces.copy())
