#! /usr/bin/env python
# -*- coding: UTF-8 -*- #
from field import BaseField

__all__ = ['ParquetTable']


class BaseTable(object):
    def __init__(self, name):
        self._name = name
        self._connection = None
        self._schema = None
        self._config = dict()
        self._fields = dict()

    @property
    def config(self):
        return self._config

    @config.setter
    def config(self, params):
        self._config = params

    @property
    def connection(self):
        return self._connection

    @connection.setter
    def connection(self, db_conn):
        self._connection = db_conn
        self._schema = self.schema()

    @property
    def name(self):
        return self._name

    @property
    def fields(self):
        return self._fields

    def schema(self):
        raise NotImplementedError()

    def __call__(self, field_name):
        return self._fields.get(field_name)


class ParquetTable(BaseTable):
    def __init__(self, name, schema_index_file=None):
        super(ParquetTable, self).__init__(name)
        self._schema_index_file = schema_index_file or '*'

    def schema(self):
        if not self._schema:
            self._schema = self._connection.schema.names
            del self._connection
        for field in self._schema:
            self._fields[field] = BaseField(field, self)
        return self._schema

    @property
    def schema_index_file(self):
        return self._schema_index_file

    @schema_index_file.setter
    def schema_index_file(self, value):
        self._schema_index_file = value
