#! /usr/bin/env python
# -*- coding: UTF-8 -*- #
from query import BaseQuery

__all__ = ['BaseField']


class BaseField(object):
    def __init__(self, name, table_obj):
        self._name = name
        self._table = table_obj
        self._invert = False

    @property
    def name(self):
        return self._name

    @property
    def table(self):
        return self._table

    @property
    def invert(self):
        return self._invert

    def _quote(self, value):
        try:
            value = int(value)
        except ValueError:
            value = "'%s'" % value
        except TypeError:
            raise TypeError('It should be string or integer')
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
                )
            )
        )

    def __ne__(self, y):
        return self._check_and_do_invert(
            BaseQuery("`%s`.`%s` <> %s" % (
                self._table.name,
                self._name,
                self._quote(y)
                )
            )
        )

    def __gt__(self, y):
        return self._check_and_do_invert(
            BaseQuery("`%s`.`%s` > %s" % (
                self._table.name,
                self._name,
                self._quote(y)
                )
            )
        )

    def __ge__(self, y):
        return self._check_and_do_invert(
            BaseQuery("`%s`.`%s` >= %s" % (
                self._table.name,
                self._name,
                self._quote(y)
                )
            )
        )

    def __lt__(self, y):
        return self._check_and_do_invert(
            BaseQuery("`%s`.`%s` < %s" % (
                self._table.name,
                self._name,
                self._quote(y)
                )
            )
        )

    def __le__(self, y):
        return self._check_and_do_invert(
            BaseQuery("`%s`.`%s` <= %s" % (
                self._table.name,
                self._name,
                self._quote(y)
                )
            )
        )

    def regexp(self, y):
        return self._check_and_do_invert(
            BaseQuery("`%s`.`%s` REGEXP %s" % (
                self._table.name,
                self._name,
                self._quote(y)
                )
            )
        )

    def __invert__(self):
        self._invert = True
        return self
