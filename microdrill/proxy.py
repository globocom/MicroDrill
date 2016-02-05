#! /usr/bin/env python
# -*- coding: UTF-8 -*- #


class ProxyDAL(object):
    def __init__(self, dals):
        self._dals = dals

    @property
    def databases(self):
        databases = []
        for dal in self._dals:
            databases += dal.databases
        return databases

    def schema(self, name):
        if name:
            for dal in self._dals:
                schema_list = dal.schema(name)
                if schema_list:
                    return schema_list
            raise ValueError('No database with this name: %s' % name)
        return []

    def configure_connection(self, name, **config):
        for dal in self._dals:
            dal.configure(name, **config)

    def connect(self, name):
        for dal in self._dals:
            dal.connect(name)
