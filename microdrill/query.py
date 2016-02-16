#! /usr/bin/env python
# -*- coding: UTF-8 -*- #

__all__ = ['BaseQuery']


class BaseQuery(object):

    def __init__(self, query="", fields=[]):
        self._query = query
        self._fields = list(fields)

    @property
    def query(self):
        return self._query.strip()

    @property
    def fields(self):
        return self._fields

    def __and__(self, y):
        return BaseQuery("(%s) AND (%s)" % (self.query, y.query),
                         self.fields + y.fields)

    def __or__(self, y):
        return BaseQuery("(%s) OR (%s)" % (self.query, y.query),
                         self.fields + y.fields)

    def __invert__(self):
        return BaseQuery("NOT (%s)" % self.query, self.fields)

    def __add__(self, y):
        if isinstance(y, str):
            return BaseQuery("%s %s" % (self.query, y), self.fields)
        elif isinstance(y, BaseQuery):
            return BaseQuery("%s %s" % (self.query, y.query),
                             self.fields + y.fields)
        else:
            raise ValueError('Only BaseQuery or String objects are added')
