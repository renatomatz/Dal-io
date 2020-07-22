"""Defines Node abstract class

Nodes are the key building blocks of your model as they represent any data
that passes thorugh it. These are usued in subsequent classes to describe and
manage data.
"""
from typing import Any, List


class _Node:
    """Node abstract class.

    Used to represent input or output data at any point of the analysis.
    Implements basic functionality to request data from a connection and
    ensure the integrity of the requested data.

    Attributes:
        _connection: an entry point for data
        tags (list): strings to describe this node
    """

    _connection: Any
    tags: List[str]

    def __init__(self):
        self._connection = None
        self.tags = []

    def request(self, **kwargs):
        """Requests data from a source

        Args:
            **kwargs (dict): arguments dependent on the data being retrieved
            by the connection of processed after request

        Returns:
            Data requested from connection, after any needed processing and
            integrity checks.
        """
        raise NotImplementedError()

    def check(self):
        """Checks if this _Node instance is ready to request data

        Returns:
            A boolean stating whether _Node instance is ready.
        """
        raise NotImplementedError()

    def describe(self):
        """Outputs a description of this _Node instance
        """
        print(f"\
            connection: {self._connection}\n \
            tags: {self.tags}\n \
        ")

    def get_connection(self):
        """Gets instance's connection"""
        return self._connection

    def set_connection(self, new_connection):
        """Set connection attribute

        This method can be further extended to certify connection and
        implement other securuty features
        """
        self._connection = new_connection
