from unittest import TestCase
from microdrill.database import BaseDatabase, ParquetDatabase
from microdrill.pool import ParquetPool
from tests.helper import FakeDatabase, FakePool


class TestBasePool(TestCase):

    def setUp(self):
        self.database = BaseDatabase('Test database', 'hdfs://...')
        self.pool = FakePool()

    def test_should_set_and_get_like_a_dict(self):
        self.pool[self.database.name] = self.database
        self.assertEqual(self.pool.get(self.database.name), self.database)

    def test_should_add_multiple_databases(self):
        self.pool[self.database.name] = self.database
        self.pool['Test database2'] = self.database
        self.pool['Test database3'] = self.database
        self.assertEqual(self.pool.get(self.database.name), self.database)
        self.assertEqual(self.pool.get('Test database2'), self.database)
        self.assertEqual(self.pool.get('Test database3'), self.database)
        self.assertEqual(len(self.pool), 3)


class TestParquetPool(TestCase):

    def setUp(self):
        self.name, self.uri = 'Test database', 'hdfs://...'
        self.pool = ParquetPool()

    def test_should_raise_value_error_with_wrong_database(self):
        database = FakeDatabase(self.name, self.uri)
        self.assertRaises(ValueError, self.pool.validate, database)

    def test_should_add_to_pool_with_right_database(self):
        database = ParquetDatabase(self.name, self.uri)
        self.pool[database.name] = database
        self.assertEqual(self.pool.get(database.name), database)
        self.assertEqual(len(self.pool), 1)
