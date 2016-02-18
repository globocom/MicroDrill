#! /usr/bin/env python
# -*- coding: UTF-8 -*- #

from unittest import TestCase

from microdrill.dal.sql import SQLDAL
from microdrill.query import BaseQuery
from tests.helper import FakeTable, factory_field


class TestSQLDAL(TestCase):

    def setUp(self):
        self.dal = SQLDAL()
        self.table = FakeTable('test_table')
        self.field = factory_field(self.table)

    def test_should_return_base_query(self):
        self.assertIsInstance(self.dal.base_query, BaseQuery)

        self.dal.select(self.field)

        self.assertIsInstance(self.dal.base_query, BaseQuery)

    def test_should_append_table_in_from_using_group_by(self):
        field1 = factory_field(FakeTable('test_table2'))
        self.dal.select(self.field).group_by(field1)

        self.assertEqual(2, self.dal.query.count('test_table2'))

    def test_should_append_table_in_from_using_where(self):
        field1 = factory_field(FakeTable('test_table2'))
        self.dal.select(self.field).where(field1 == 2)

        self.assertEqual(2, self.dal.query.count('test_table2'))

    def test_should_append_table_in_from_using_having(self):
        field1 = factory_field(FakeTable('test_table2'))
        self.dal.select(self.field).having(field1 == 2)

        self.assertEqual(2, self.dal.query.count('test_table2'))

    def test_should_return_dal_when_call_select(self):
        self.assertEqual(id(self.dal), id(self.dal.select()))

    def test_should_return_dal_when_call_where(self):
        self.assertEqual(id(self.dal), id(self.dal.where()))

    def test_should_return_dal_when_call_order_by(self):
        self.assertEqual(id(self.dal), id(self.dal.order_by()))

    def test_should_return_dal_when_call_group_by(self):
        self.assertEqual(id(self.dal), id(self.dal.group_by()))

    def test_should_return_dal_when_call_limit(self):
        self.assertEqual(id(self.dal), id(self.dal.limit(10)))

    def test_should_return_dal_when_call_having(self):
        self.assertEqual(id(self.dal), id(self.dal.having()))

    def test_should_return_query_for_select_field(self):
        self.dal.select(self.field)

        expected = "SELECT `test_table`.`My_Field` FROM test_table"
        self.assertEqual(expected, self.dal.query)

    def test_should_return_query_for_select_multiple_fields(self):
        field1 = factory_field(self.table)
        field2 = factory_field(self.table)
        self.dal.select(self.field, field1, field2)

        expected = "SELECT `test_table`.`My_Field`, `test_table`.`My_Field1`, `test_table`.`My_Field2` FROM test_table"
        self.assertEqual(expected, self.dal.query)

    def test_should_return_query_for_where_fields(self):
        self.dal.select(self.field).where(self.field==1)

        expected = "SELECT `test_table`.`My_Field` FROM test_table WHERE `test_table`.`My_Field` = 1"
        self.assertEqual(expected, self.dal.query)

    def test_should_return_query_for_where_multiple_fields(self):
        field1 = factory_field(self.table)
        field2 = factory_field(self.table)
        self.dal.select(self.field).where(~(field1 == 2) &(field2 != 1))

        expected = "SELECT `test_table`.`My_Field` FROM test_table WHERE (NOT (`test_table`.`My_Field1` = 2)) AND (`test_table`.`My_Field2` <> 1)"
        self.assertEqual(expected, self.dal.query)

    def test_should_return_query_for_order_by_fields(self):
        self.dal.select(self.field).order_by(self.field)

        expected = "SELECT `test_table`.`My_Field` FROM test_table ORDER BY `test_table`.`My_Field` ASC"
        self.assertEqual(expected, self.dal.query)

    def test_should_return_query_for_order_by_multiple_fields(self):
        field1 = factory_field(self.table)
        field2 = factory_field(self.table)
        self.dal.select(self.field).order_by(~field1, field2)

        expected = "SELECT `test_table`.`My_Field` FROM test_table ORDER BY `test_table`.`My_Field1` DESC, `test_table`.`My_Field2` ASC"
        self.assertEqual(expected, self.dal.query)

    def test_should_return_query_for_group_by_fields(self):
        self.dal.select(self.field).group_by(self.field)

        expected = "SELECT `test_table`.`My_Field` FROM test_table GROUP BY `test_table`.`My_Field`"
        self.assertEqual(expected, self.dal.query)

    def test_should_return_query_for_group_by_multiple_fields(self):
        field1 = factory_field(self.table)
        field2 = factory_field(self.table)
        self.dal.select(self.field).group_by(~field1, field2)

        expected = "SELECT `test_table`.`My_Field` FROM test_table GROUP BY `test_table`.`My_Field1`, `test_table`.`My_Field2`"
        self.assertEqual(expected, self.dal.query)

    def test_should_return_query_for_limit_field(self):
        self.dal.select(self.field).limit(10)

        expected = "SELECT `test_table`.`My_Field` FROM test_table LIMIT 10"
        self.assertEqual(expected, self.dal.query)

    def test_should_return_query_for_having_fields(self):
        self.dal.select(self.field).having(self.field==1)

        expected = "SELECT `test_table`.`My_Field` FROM test_table HAVING `test_table`.`My_Field` = 1"
        self.assertEqual(expected, self.dal.query)

    def test_should_return_query_for_having_multiple_fields(self):
        field1 = factory_field(self.table)
        field2 = factory_field(self.table)
        self.dal.select(self.field).having(~(field1 == 2) &(field2 != 1))

        expected = "SELECT `test_table`.`My_Field` FROM test_table HAVING (NOT (`test_table`.`My_Field1` = 2)) AND (`test_table`.`My_Field2` <> 1)"
        self.assertEqual(expected, self.dal.query)

    def test_should_create_query_in_correct_order(self):
        self.dal.limit(2)
        self.dal.group_by(self.field)
        self.dal.order_by(self.field)
        self.dal.where(self.field == 1)
        self.dal.select(self.field)
        self.dal.having(self.field == 22)

        expected = "SELECT `test_table`.`My_Field` FROM test_table WHERE `test_table`.`My_Field` = 1 GROUP BY `test_table`.`My_Field` HAVING `test_table`.`My_Field` = 22 ORDER BY `test_table`.`My_Field` ASC LIMIT 2"
        self.assertEqual(expected, self.dal.query)
