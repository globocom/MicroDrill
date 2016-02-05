from unittest import TestCase
from microdrill.database import BaseDatabase, ParquetDatabase


class TestGenericDatabase(TestCase):

    def test_should_set_right_parameters(self):
        name, uri = 'Test Name', 'hdfs://...'
        database = BaseDatabase(name, uri)
        self.assertEqual(database._name, name)
        self.assertEqual(database._uri, uri)


class TestParquetDatabase(TestCase):

    def test_should_set_uri_to_schema_index_file_with_asterisk(self):
        name, uri = 'Test Name', 'hdfs://...'
        database = ParquetDatabase(name, uri)
        self.assertEqual(database.schema_index_file, '%s/*' % uri)
