#! /usr/bin/env python
# -*- coding: UTF-8 -*- #
from query import BaseQuery

__all__ = ['BaseField']


class BaseField(object):
    def __init__(self, name, table_obj):
        self._name = name
        self._table = table_obj

    @property
    def name(self):
        return self._name

    @property
    def table(self):
        return self._table

    def _quote(self, value):
        try:
            value = int(value)
        except ValueError:
            value = "'%s'" % value
        except TypeError:
            raise TypeError('It should be string or integer')
        return value

    def __eq__(self, y):
        return BaseQuery("`%s`.`%s` = %s" % (
            self._table.name,
            self._name,
            self._quote(y)
            )
        )

    def __ne__(self, y):
        return BaseQuery("`%s`.`%s` <> %s" % (
            self._table.name,
            self._name,
            self._quote(y)
            )
        )

    def __gt__(self, y):
        return BaseQuery("`%s`.`%s` > %s" % (
            self._table.name,
            self._name,
            self._quote(y)
            )
        )

    def __ge__(self, y):
        return BaseQuery("`%s`.`%s` >= %s" % (
            self._table.name,
            self._name,
            self._quote(y)
            )
        )

    def __lt__(self, y):
        return BaseQuery("`%s`.`%s` < %s" % (
            self._table.name,
            self._name,
            self._quote(y)
            )
        )

    def __le__(self, y):
        return BaseQuery("`%s`.`%s` <= %s" % (
            self._table.name,
            self._name,
            self._quote(y)
            )
        )

    def regexp(self, y):
        return BaseQuery("`%s`.`%s` REGEXP %s" % (
            self._table.name,
            self._name,
            self._quote(y)
            )
        )
