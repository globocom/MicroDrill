#! /usr/bin/env python
# -*- coding: UTF-8 -*- #

from microdrill.query import BaseQuery
from microdrill.dal import BaseDAL


class SQLDAL(BaseDAL):
    def __init__(self):
        super(SQLDAL, self).__init__()
        self._query = {}

    @property
    def base_query(self):
        return (
            self._query.get('select', BaseQuery()) +
            self._from() +
            self._query.get('where', BaseQuery()) +
            self._query.get('group_by', BaseQuery()) +
            self._query.get('having', BaseQuery()) +
            self._query.get('order_by', BaseQuery()) +
            self._query.get('limit', BaseQuery())
        )

    @property
    def query(self):
        return self.base_query.query

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

    def limit(self, limit):
        query = "LIMIT %s" % limit
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
