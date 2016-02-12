import os
import shutil
import pandas as pd
from pyspark import SparkContext
from mock import patch
from unittest import TestCase
from tests.helper import FakeTable
from microdrill.table import ParquetTable
from microdrill.dal import BaseDAL, ParquetDAL
from microdrill.field import BaseField


class TestBaseDal(TestCase):

    def setUp(self):
        self.dal = BaseDAL()
        self.table = FakeTable('test_table')

    def test_should_set_table_in_dal(self):
        self.dal.set_table(self.table.name, self.table)
        self.assertEqual(self.dal.tables.get(self.table.name), self.table)
        self.assertEqual(len(self.dal.tables), 1)

    def test_should_overwrite_table_in_dal_with_same_name(self):
        table2 = FakeTable('test_table')
        self.dal.set_table(self.table.name, self.table)
        self.dal.set_table(table2.name, table2)
        self.assertEqual(self.dal.tables.get(self.table.name), table2)
        self.assertEqual(len(self.dal.tables), 1)

    def test_should_set_multiple_tables_in_dal(self):
        table2 = FakeTable('test_table2')
        table3 = FakeTable('test_table3')
        self.dal.set_table(self.table.name, self.table)
        self.dal.set_table(table2.name, table2)
        self.dal.set_table(table3.name, table3)
        self.assertEqual(self.dal.tables.get(self.table.name), self.table)
        self.assertEqual(self.dal.tables.get(table2.name), table2)
        self.assertEqual(self.dal.tables.get(table3.name), table3)
        self.assertEqual(len(self.dal.tables), 3)

    def test_should_configure_correct_table_by_name(self):
        table2 = FakeTable('test_table2')
        self.dal.set_table(self.table.name, self.table)
        self.dal.set_table(table2.name, table2)
        self.dal.configure(self.table.name, make_happy='Ok')
        self.assertEqual(self.table.config.get('make_happy'), 'Ok')
        self.assertEqual(table2.config.get('make_happy'), None)

    def test_should_overwrite_configuration_when_configure_called(self):
        self.dal.set_table(self.table.name, self.table)
        self.dal.configure(self.table.name, make_sad=':(')
        self.dal.configure(self.table.name, make_happy=':)')
        self.assertEqual(self.table.config.get('make_happy'), ':)')
        self.assertEqual(self.table.config.get('make_sad'), None)

    def test_should_return_table_calling_dal(self):
        self.dal.set_table(self.table.name, self.table)
        self.assertIs(self.dal(self.table.name), self.table)

    def test_should_return_field_calling_dal_twice(self):
        name = 'My_Field'
        field = BaseField(name, self.table)
        self.table._fields[name] = field
        self.dal.set_table(self.table.name, self.table)
        self.assertIs(self.dal(self.table.name)(name), field)


class TestParquetDal(TestCase):

    def setUp(self):
        self.sc = SparkContext._active_spark_context
        self.dirname = os.path.dirname(os.path.abspath(__file__))
        self.dal = ParquetDAL(self.dirname, self.sc)
        self.filename = "example.parquet"
        self.full_path_file = os.path.join(self.dirname, self.filename)
        self.dataframe = {'A': [1], 'B': [2], 'C': [3]}
        self.df = pd.DataFrame(self.dataframe)
        self.spark_df = self.dal.sql.createDataFrame(self.df,
                                                     self.dataframe.keys())

        self.spark_df.write.parquet(self.full_path_file)

    def test_should_get_schema_from_parquet(self):

        table = ParquetTable('test_table', schema_index_file=self.filename)
        self.dal.set_table(table.name, table)
        self.assertEqual(table.schema(), self.dataframe.keys())

    @patch('microdrill.dal.SQLContext.read')
    def test_should_not_connect_twice_on_next_get_schema_from_parquet(self,
                                                                      mock_df):
        dal = ParquetDAL(self.dirname, self.sc)
        table = ParquetTable('test_table', schema_index_file=self.filename)
        dal.set_table(table.name, table)  # here get schema too

        self.assertTrue(mock_df.parquet.called)
        mock_df.reset_mock()
        table.schema()
        self.assertFalse(mock_df.parquet.called)

    @patch('microdrill.dal.SQLContext.read')
    def test_should_connect(self, mock_df):
        dal = ParquetDAL(self.dirname, self.sc)
        table = ParquetTable('test_table', schema_index_file=self.filename)
        dal.set_table(table.name, table)
        mock_df.reset_mock()
        dal.connect(table.name)
        self.assertTrue(mock_df.parquet.called)

    def tearDown(self):
        shutil.rmtree(self.full_path_file)
