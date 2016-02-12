#! /usr/bin/env python
# -*- coding: UTF-8 -*- #

__all__ = ['BaseQuery']


class BaseQuery(object):

    def __init__(self, query):
        self._query = query

    @property
    def query(self):
        return self._query

    def __and__(self, y):
        return BaseQuery("(%s) AND (%s)" % (self._query, y.query))

    def __or__(self, y):
        return BaseQuery("(%s) OR (%s)" % (self._query, y.query))

    def __invert__(self):
        return BaseQuery("NOT (%s)" % self._query)

    def __add__(self, y):
        if isinstance(y, str):
            return BaseQuery("%s %s" % (self._query, y))
        elif isinstance(y, BaseQuery):
            return BaseQuery("%s %s" % (self._query, y.query))
        else:
            raise ValueError('Only BaseQuery or String objects are added')
