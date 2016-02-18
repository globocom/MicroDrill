#! /usr/bin/env python
# -*- coding: UTF-8 -*- #
from query import BaseQuery

__all__ = ['BaseField']


class BaseField(object):
    def __init__(self, name, table_obj, sql_template=None, invert=False):
        self._name = name
        self._table = table_obj
        self._invert = invert
        self._sql_template = sql_template

    @property
    def name(self):
        return self._name

    @property
    def table(self):
        return self._table

    @property
    def invert(self):
        return self._invert

    @property
    def avg(self):
        return BaseField(self._name, self._table, sql_template='AVG(%s)')

    @property
    def count(self):
        return BaseField(self._name, self._table, sql_template='COUNT(%s)')

    @property
    def sum(self):
        return BaseField(self._name, self._table, sql_template='SUM(%s)')

    @property
    def sql_template(self):
        return self._sql_template

    @sql_template.setter
    def sql_template(self, sql_template):
        self._sql_template = sql_template

    def sql(self, extra_template=None):
        sql = "`%s`.`%s`" % (self.table.name, self.name)
        if self.sql_template:
            sql = self.sql_template % sql

        if extra_template:
            sql = extra_template % sql

        return sql

    def _quote(self, value):
        try:
            value = int(value)
        except ValueError:
            value = "'%s'" % value
        except TypeError:
            raise TypeError('It should be string or integer, type %s found' % type(value))
        return value

    def _check_and_do_invert(self, base_query):
        if self.invert:
            base_query = ~base_query
        return base_query

    def __eq__(self, y):
        return self._check_and_do_invert(
            BaseQuery("`%s`.`%s` = %s" % (
                self._table.name,
                self._name,
                self._quote(y)
                ), [self]
            )
        )

    def __ne__(self, y):
        return self._check_and_do_invert(
            BaseQuery("`%s`.`%s` <> %s" % (
                self._table.name,
                self._name,
                self._quote(y)
                ), [self]
            )
        )

    def __gt__(self, y):
        return self._check_and_do_invert(
            BaseQuery("`%s`.`%s` > %s" % (
                self._table.name,
                self._name,
                self._quote(y)
                ), [self]
            )
        )

    def __ge__(self, y):
        return self._check_and_do_invert(
            BaseQuery("`%s`.`%s` >= %s" % (
                self._table.name,
                self._name,
                self._quote(y)
                ), [self]
            )
        )

    def __lt__(self, y):
        return self._check_and_do_invert(
            BaseQuery("`%s`.`%s` < %s" % (
                self._table.name,
                self._name,
                self._quote(y)
                ), [self]
            )
        )

    def __le__(self, y):
        return self._check_and_do_invert(
            BaseQuery("`%s`.`%s` <= %s" % (
                self._table.name,
                self._name,
                self._quote(y)
                ), [self]
            )
        )

    def regexp(self, y):
        return self._check_and_do_invert(
            BaseQuery("`%s`.`%s` REGEXP %s" % (
                self._table.name,
                self._name,
                self._quote(y)
                ), [self]
            )
        )

    def __invert__(self):
        return BaseField(self._name, self._table, invert=True)
