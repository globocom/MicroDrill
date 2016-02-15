#! /usr/bin/env python
# -*- coding: UTF-8 -*- #

__all__ = ['BaseQuery']


class BaseQuery(object):

    def __init__(self, query=""):
        self._query = query

    @property
    def query(self):
        return self._query.strip()

    def __and__(self, y):
        return BaseQuery("(%s) AND (%s)" % (self.query, y.query))

    def __or__(self, y):
        return BaseQuery("(%s) OR (%s)" % (self.query, y.query))

    def __invert__(self):
        return BaseQuery("NOT (%s)" % self.query)

    def __add__(self, y):
        if isinstance(y, str):
            return BaseQuery("%s %s" % (self.query, y))
        elif isinstance(y, BaseQuery):
            return BaseQuery("%s %s" % (self.query, y.query))
        else:
            raise ValueError('Only BaseQuery or String objects are added')
