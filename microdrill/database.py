#! /usr/bin/env python
# -*- coding: UTF-8 -*- #

__all__ = ['ParquetDatabase']


class BaseDatabase(object):
    def __init__(self, name, uri):
        self._name = name
        self._uri = uri
        self._config = dict()

    @property
    def config(self):
        return self._config

    @config.setter
    def config(self, params):
        self._config = params

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def uri(self):
        return self._uri

    @uri.setter
    def uri(self, value):
        self._uri = value


class ParquetDatabase(BaseDatabase):
    def __init__(self, name, uri, schema_index_file=None):
        super(ParquetDatabase, self).__init__(name, uri)
        self.schema_index_file = schema_index_file or "%s*" % self.uri
