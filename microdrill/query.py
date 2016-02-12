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
