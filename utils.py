from itertools import izip_longest
import os


def dict_to_str(dict):
    """Output a dict as a string, with formatting niceties"""
    return str(dict).rstrip("}").lstrip("{")


def grouper(iterable, n, fillvalue=None):
    """Collect data into fixed-length chunks or blocks
    Stolen from: https://docs.python.org/2/library/itertools.html#recipes

    e.g.
    >>> grouper('ABCDEFG', 3, 'x')
    ['ABC', 'DEF', 'Gxx']
    """
    args = [iter(iterable)] * n
    return izip_longest(fillvalue=fillvalue, *args)


class BasicObj(object):
    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, dict_to_str(self.__dict__))

    def __str__(self):
        return "%s(%s)" % (self.__class__.__name__, dict_to_str(self.__dict__))


def sanitise_path(filepath):
    """Resolve symlinks, and form absolute path.

    I can never remember which bit of os.path to use.
    """
    return os.path.realpath(filepath)

