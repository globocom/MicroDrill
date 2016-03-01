#! /usr/bin/env python
# -*- coding: UTF-8 -*- #

from microdrill.pool import ParquetPool
from pyspark.sql import SQLContext
from microdrill.dal.sql import SQLDAL


class ParquetDAL(SQLDAL):
    def __init__(self, uri, *args, **kwargs):
        super(ParquetDAL, self).__init__()
        self._tables = ParquetPool()
        self._uri = uri
        self._context = SQLContext(*args, **kwargs)

    def set_table(self, table_obj):
        super(ParquetDAL, self).set_table(table_obj)
        self._connect_for_schema(table_obj.name)

    def _connect_for_schema(self, name):
        table = self._tables.get(name)
        if table:
            table.config['files'] = [table.schema_index_file]
            table.connection = self.connect(name)
        else:
            raise ValueError("Table %s not found" % name)

    def execute(self):

        for table_name in [field.table.name for field in self.base_query.fields]:
            self.connect(table_name).registerTempTable(table_name)
        result = self._context.sql(self.query)
        self._query = {}

        return result

    def connect(self, name):
        table = self._tables.get(name)

        if table and table.config.get('files'):
            files = table.config.get('files')
            parquet_list = list()
            for filename in files:
                parquet_list.append("%s/%s/%s" % (self._uri,
                                                  table.name,
                                                  filename))
            return self._context.read.parquet(*parquet_list)
        raise ValueError("Table (%s) and files needed" % name)
