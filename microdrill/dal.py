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

    def __call__(self, table_name):
        return self._tables.get(table_name)

    @property
    def base_query(self):
        return (
            self._query.get('select', BaseQuery()) +
            self._from() +
            self._query.get('where', BaseQuery()) +
            self._query.get('order_by', BaseQuery()) +
            self._query.get('group_by', BaseQuery()) +
            self._query.get('having', BaseQuery()) +
            self._query.get('limit', BaseQuery())
        )

    @property
    def query(self):
        return self.base_query.query

    @property
    def sql(self):
        return self._sql

    @property
    def tables(self):
        return self._tables

    def configure(self, name, **params):
        table = self._tables.get(name)
        if table:
            table.config = params

    def connect(self, *args, **kwargs):
        raise NotImplementedError()

    def execute(self):
        raise NotImplementedError()

    def set_table(self, name, table_obj):
        self._tables[name] = table_obj

    def group_by(self, *fields):
        self._query['group_by'] = self._make_simple_statement(
            BaseQuery("GROUP BY", fields)
        )

        return self

    def having(self, *base_queries):
        self._query['having'] = self._make_conditional_statement(
            BaseQuery("HAVING"), base_queries
        )

        return self

    def limit(self, end_limit, start_limit=0):
        query = "LIMIT %s, %s" % (start_limit, end_limit)
        self._query['limit'] = BaseQuery(query)

        return self

    def order_by(self, *fields):
        self._query['order_by'] = self._make_simple_statement(
            BaseQuery("ORDER BY", fields),
            extra_action=self._treat_order_by
        )

        return self

    def select(self, *fields):
        self._query['select'] = self._make_simple_statement(
            BaseQuery("SELECT", fields)
        )

        return self

    def where(self, *base_queries):
        self._query['where'] = self._make_conditional_statement(
            BaseQuery("WHERE"), base_queries
        )

        return self

    def _from(self):
        tables = []
        for query in self._query.values():
            tables += [field.table.name for field in query.fields]

        query = BaseQuery("FROM")
        query += BaseQuery(", ".join(set(tables)))
        return query

    def _make_conditional_statement(self, query, base_queries):
        for base_query in base_queries:
            query += base_query

        return query

    def _make_simple_statement(self, base_query, extra_action=None):
        query_fields = []
        for field in base_query.fields:

            sql_name = field.sql()
            if extra_action:
                sql_name = extra_action(field)

            query_fields.append(sql_name)

        base_query += BaseQuery(", ".join(query_fields))
        return base_query

    def _treat_order_by(self, field):
        sql = field.sql('%s ASC')

        if field.invert:
            sql = field.sql('%s DESC')

        return sql


class ParquetDAL(BaseDAL):
    def __init__(self, uri, *args, **kwargs):
        super(ParquetDAL, self).__init__()
        self._tables = ParquetPool()
        self._uri = uri
        self._sql = SQLContext(*args, **kwargs)

    def set_table(self, name, table_obj):
        super(ParquetDAL, self).set_table(name, table_obj)
        self._connect_for_schema(name)

    def _connect_for_schema(self, name):
        table = self._tables.get(name)
        if table:
            table.config['files'] = [table.schema_index_file]
            table.connection = self.connect(name)
        else:
            raise ValueError("Table %s not found" % name)

    def execute(self):
        for field in self.base_query.fields:
            self.connect(field.table.name).registerTempTable(field.table.name)
        sql = self._sql.sql(self.query)
        self._query = {}

        return sql

    def connect(self, name):
        table = self._tables.get(name)
        files = table.config.get('files')

        if table and files:
            parquet_list = list()
            for filename in files:
                parquet_list.append("%s/%s/%s" % (self._uri,
                                                  table.name,
                                                  filename))
            return self._sql.read.parquet(*parquet_list)
        raise ValueError("Table (%s) and files needed" % name)
