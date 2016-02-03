#! /usr/bin/env python
# -*- coding: UTF-8 -*- #

from pool import ParquetPool
from pyspark.sql import SQLContext

__all__ = ['ParquetDAL']


class BaseDAL(SQLContext):
    def __init__(self, *args, **kwargs):
        super(BaseDAL, self).__init__(*args, **kwargs)
        self._connections = dict()
        self._schemas = dict()
        self._databases = dict()

    @property
    def databases(self):
        return self._databases

    def schema(self, name):
        raise NotImplementedError()

    def set_database(self, name, p_connection):
        self._databases[name] = p_connection

    def configure(self, name, **params):
        database = self._databases.get(name)
        if database:
            database.config = params


class ParquetDAL(BaseDAL):
    def __init__(self, *args, **kwargs):
        super(ParquetDAL, self).__init__(*args, **kwargs)
        self._databases = ParquetPool()

    def _connect_for_schema(self, name):
        database = self._databases.get(name)
        if database:
            self.configure(name, files=[database.schema_index_file])
            self.connect(name)

    def schema(self, name):
        if name and not self._schemas.get(name):
            self._connect_for_schema(name)
            self._schemas[name] = self._connections[name].schema.names
        return self._schemas.get(name, [])

    def configure(self, name, files=list(), **params):
        super(ParquetDAL, self).configure(name, files=files, **params)

    def connect(self, name):
        database = self._databases.get(name)
        if database:
            parquet_list = list()
            for filename in database.config['files']:
                parquet_list.append("%s/%s" % (self._databases[name].uri,
                                               filename))
            self._connections[name] = self.read.parquet(*parquet_list)
