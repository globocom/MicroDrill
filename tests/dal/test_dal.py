#! /usr/bin/env python
# -*- coding: UTF-8 -*- #

from unittest import TestCase

from microdrill.dal import BaseDAL
from tests.helper import FakeTable, factory_field


class TestBaseDal(TestCase):

    def setUp(self):
        self.dal = BaseDAL()
        self.table = FakeTable('test_table')

    def test_should_set_table_in_dal(self):
        self.dal.set_table(self.table)

        self.assertEqual(self.dal.tables.get(self.table.name), self.table)
        self.assertEqual(1, len(self.dal.tables))

    def test_should_overwrite_table_in_dal_with_same_name(self):
        table2 = FakeTable('test_table')
        self.dal.set_table(self.table)
        self.dal.set_table(table2)

        self.assertEqual(self.dal.tables.get(self.table.name), table2)
        self.assertEqual(1, len(self.dal.tables))

    def test_should_set_multiple_tables_in_dal(self):
        table2 = FakeTable('test_table2')
        table3 = FakeTable('test_table3')
        self.dal.set_table(self.table)
        self.dal.set_table(table2)
        self.dal.set_table(table3)

        self.assertEqual(self.dal.tables.get(self.table.name), self.table)
        self.assertEqual(self.dal.tables.get(table2.name), table2)
        self.assertEqual(self.dal.tables.get(table3.name), table3)
        self.assertEqual(3, len(self.dal.tables))

    def test_should_configure_correct_table_by_name(self):
        table2 = FakeTable('test_table2')
        self.dal.set_table(self.table)
        self.dal.set_table(table2)
        self.dal.configure(self.table.name, make_happy='Ok')

        self.assertEqual('Ok', self.table.config.get('make_happy'))
        self.assertEqual(None, table2.config.get('make_happy'))

    def test_should_overwrite_configuration_when_configure_called(self):
        self.dal.set_table(self.table)
        self.dal.configure(self.table.name, make_sad=':(')
        self.dal.configure(self.table.name, make_happy=':)')

        self.assertEqual(':)', self.table.config.get('make_happy'))
        self.assertEqual(None, self.table.config.get('make_sad'))

    def test_should_return_table_calling_dal(self):
        self.dal.set_table(self.table)

        self.assertIs(self.dal(self.table.name), self.table)

    def test_should_return_field_calling_dal_twice(self):
        field = factory_field(self.table)
        self.dal.set_table(self.table)

        self.assertIs(field, self.dal(self.table.name)('My_Field'))
