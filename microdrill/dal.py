#! /usr/bin/env python
# -*- coding: UTF-8 -*- #

from pool import ParquetPool
from pyspark.sql import SQLContext
from query import BaseQuery

__all__ = ['ParquetDAL']


class BaseDAL(object):
    def __init__(self):
        self._connections = dict()
        self._tables = dict()
        self._sql = None
        self._uri = None
        self._query = {}

    @property
    def tables(self):
        return self._tables

    @property
    def sql(self):
        return self._sql

    @property
    def query(self):
        return (
            self._query.get('select', BaseQuery()) + 
            self._query.get('where', BaseQuery()) +
            self._query.get('order_by', BaseQuery()) +
            self._query.get('group_by', BaseQuery())
        ).query

    def connect(self, *args, **kwargs):
        raise NotImplementedError()

    def __call__(self, table_name):
        return self._tables.get(table_name)

    def set_table(self, name, table_obj):
        self._tables[name] = table_obj

    def configure(self, name, **params):
        table = self._tables.get(name)
        if table:
            table.config = params

    def select(self, *fields):
        select_query = []
        from_query = []

        for field in fields:
            field_value = "`%s`.`%s`" % (field.table.name, field.name)
            select_query.append(field_value)
            from_query.append(field.table.name)

        query = BaseQuery("SELECT")
        query += BaseQuery(", ".join(select_query))
        query += BaseQuery("FROM")
        query += BaseQuery(", ".join(set(from_query)))

        self._query['select'] = query

        return self

    def where(self, *base_queries):
        query = BaseQuery("WHERE")
        for base_query in base_queries:
            query += base_query

        self._query['where'] = query

        return self

    def order_by(self, *fields):
        ordered = []
        for field in fields:
            order = 'ASC'
            if field.invert:
                order = 'DESC'

            ordered.append("`%s`.`%s` %s" % (field.table.name, field.name, 
                                             order))

        query = BaseQuery("ORDER BY")
        query += BaseQuery(", ".join(ordered))
        self._query['order_by'] = query

        return self

    def group_by(self, *fields):
        ordered = []
        for field in fields:
            ordered.append("`%s`.`%s`" % (field.table.name, field.name))

        query = BaseQuery("GROUP BY")
        query += BaseQuery(", ".join(ordered))
        self._query['group_by'] = query

        return self


class ParquetDAL(BaseDAL):
    def __init__(self, uri, *args, **kwargs):
        super(ParquetDAL, self).__init__()
        self._tables = ParquetPool()
        self._uri = uri
        self._sql = SQLContext(*args, **kwargs)

    def set_table(self, name, table_obj):
        super(ParquetDAL, self).set_table(name, table_obj)
        table_obj.connection = self._connect_for_schema(name)

    def _connect_for_schema(self, name):
        table = self._tables.get(name)
        if table:
            table.config['files'] = [table.schema_index_file]
            return self.connect(name)
        raise ValueError("Table %s not found" % name)

    def connect(self, name):
        table = self._tables.get(name)
        files = table.config.get('files')

        if table and files:
            parquet_list = list()
            for filename in files:
                parquet_list.append("%s/%s" % (self._uri,
                                               filename))
            return self._sql.read.parquet(*parquet_list)
        raise ValueError("Table (%s) and files needed" % name)
