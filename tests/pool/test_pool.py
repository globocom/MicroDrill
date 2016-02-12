from unittest import TestCase
from microdrill.table import BaseTable, ParquetTable
from microdrill.pool import ParquetPool
from tests.helper import FakeTable, FakePool


class TestBasePool(TestCase):

    def setUp(self):
        self.table = BaseTable('Test table')
        self.pool = FakePool()

    def test_should_set_and_get_like_a_dict(self):
        self.pool[self.table.name] = self.table
        self.assertEqual(self.pool.get(self.table.name), self.table)

    def test_should_add_multiple_tables(self):
        self.pool[self.table.name] = self.table
        self.pool['Test table2'] = self.table
        self.pool['Test table3'] = self.table
        self.assertEqual(self.pool.get(self.table.name), self.table)
        self.assertEqual(self.pool.get('Test table2'), self.table)
        self.assertEqual(self.pool.get('Test table3'), self.table)
        self.assertEqual(len(self.pool), 3)


class TestParquetPool(TestCase):

    def setUp(self):
        self.name = 'Test table'
        self.pool = ParquetPool()

    def test_should_raise_value_error_with_wrong_table(self):
        table = FakeTable(self.name)
        self.assertRaises(ValueError, self.pool.validate, table)

    def test_should_add_to_pool_with_right_table(self):
        table = ParquetTable(self.name)
        self.pool[table.name] = table
        self.assertEqual(self.pool.get(table.name), table)
        self.assertEqual(len(self.pool), 1)
