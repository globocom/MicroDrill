from unittest import TestCase
from microdrill.table import BaseTable, ParquetTable
from microdrill.field import BaseField


class TestGenericDatabase(TestCase):

    def setUp(self):
        self.name = 'Test_Name'
        self.table = BaseTable(self.name)

    def test_should_set_right_parameters(self):
        self.assertEqual(self.table._name, self.name)

    def test_should_get_field_calling_table(self):
        name = 'My_Field'
        field = BaseField(name, self.table)
        self.table._fields[name] = field
        self.assertIs(field, self.table(name))


class TestParquetTable(TestCase):

    def test_should_set_uri_to_schema_index_file_with_asterisk(self):
        name = 'Test Name'
        table = ParquetTable(name)
        self.assertEqual(table.schema_index_file, '*')
