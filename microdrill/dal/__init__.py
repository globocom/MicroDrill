#! /usr/bin/env python
# -*- coding: UTF-8 -*- #


class BaseDAL(object):
    def __init__(self):
        self._connections = dict()
        self._tables = dict()
        self._context = None
        self._uri = None

    def __call__(self, table_name):
        return self._tables.get(table_name)

    @property
    def context(self):
        return self._context

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

    def set_table(self, table_obj):
        self._tables[table_obj.name] = table_obj
