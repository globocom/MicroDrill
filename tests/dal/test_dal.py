import os
import shutil
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import dataframe
from collections import OrderedDict
from mock import patch
from unittest import TestCase
from tests.helper import FakeTable, factory_field
from microdrill.table import ParquetTable
from microdrill.dal import BaseDAL, ParquetDAL
from microdrill.field import BaseField
from microdrill.query import BaseQuery


class TestBaseDal(TestCase):

    def setUp(self):
        self.dal = BaseDAL()
        self.table = FakeTable('test_table')
        self.field = factory_field(self.table)

    def test_should_set_table_in_dal(self):
        self.dal.set_table(self.table.name, self.table)

        self.assertEqual(self.dal.tables.get(self.table.name), self.table)
        self.assertEqual(1, len(self.dal.tables))

    def test_should_overwrite_table_in_dal_with_same_name(self):
        table2 = FakeTable('test_table')
        self.dal.set_table(self.table.name, self.table)
        self.dal.set_table(table2.name, table2)

        self.assertEqual(self.dal.tables.get(self.table.name), table2)
        self.assertEqual(1, len(self.dal.tables))

    def test_should_set_multiple_tables_in_dal(self):
        table2 = FakeTable('test_table2')
        table3 = FakeTable('test_table3')
        self.dal.set_table(self.table.name, self.table)
        self.dal.set_table(table2.name, table2)
        self.dal.set_table(table3.name, table3)

        self.assertEqual(self.dal.tables.get(self.table.name), self.table)
        self.assertEqual(self.dal.tables.get(table2.name), table2)
        self.assertEqual(self.dal.tables.get(table3.name), table3)
        self.assertEqual(3, len(self.dal.tables))

    def test_should_configure_correct_table_by_name(self):
        table2 = FakeTable('test_table2')
        self.dal.set_table(self.table.name, self.table)
        self.dal.set_table(table2.name, table2)
        self.dal.configure(self.table.name, make_happy='Ok')

        self.assertEqual('Ok', self.table.config.get('make_happy'))
        self.assertEqual(None, table2.config.get('make_happy'))

    def test_should_overwrite_configuration_when_configure_called(self):
        self.dal.set_table(self.table.name, self.table)
        self.dal.configure(self.table.name, make_sad=':(')
        self.dal.configure(self.table.name, make_happy=':)')

        self.assertEqual(':)', self.table.config.get('make_happy'))
        self.assertEqual(None, self.table.config.get('make_sad'))

    def test_should_return_table_calling_dal(self):
        self.dal.set_table(self.table.name, self.table)

        self.assertIs(self.dal(self.table.name), self.table)

    def test_should_return_field_calling_dal_twice(self):
        self.dal.set_table(self.table.name, self.table)

        self.assertIs(self.field, self.dal(self.table.name)('My_Field'))

    def test_should_return_query_for_select_field(self):
        self.dal.select(self.field)

        expected = "SELECT `test_table`.`My_Field` FROM test_table"
        self.assertEqual(expected, self.dal.query)

    def test_should_return_base_query_for_select_field(self):
        self.dal.select(self.field)

        self.assertIsInstance(self.dal.base_query, BaseQuery)

    def test_should_return_query_for_select_multiple_fields(self):
        field1 = factory_field(self.table)
        field2 = factory_field(self.table)
        self.dal.select(self.field, field1, field2)

        expected = "SELECT `test_table`.`My_Field`, `test_table`.`My_Field1`, `test_table`.`My_Field2` FROM test_table"
        self.assertEqual(expected, self.dal.query)

    def test_should_return_dal_when_call_select(self):
        self.assertEqual(id(self.dal), id(self.dal.select()))

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

    def test_should_return_dal_when_call_where(self):
        self.assertEqual(id(self.dal), id(self.dal.where()))

    def test_should_return_dal_when_call_order_by(self):
        self.assertEqual(id(self.dal), id(self.dal.order_by()))

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

    def test_should_return_dal_when_call_group_by(self):
        self.assertEqual(id(self.dal), id(self.dal.group_by()))

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

    def test_should_append_table_in_from_using_group_by(self):
        field1 = factory_field(FakeTable('test_table2'))
        self.dal.select(self.field).group_by(field1)

        self.assertEqual(2, self.dal.query.count('test_table2'))

    def test_should_append_table_in_from_using_where(self):
        field1 = factory_field(FakeTable('test_table2'))
        self.dal.select(self.field).where(field1 == 2)

        self.assertEqual(2, self.dal.query.count('test_table2'))

    def test_should_create_query_in_correct_order(self):
        self.dal.group_by(self.field)
        self.dal.order_by(self.field)
        self.dal.where(self.field == 1)
        self.dal.select(self.field)

        expected = "SELECT `test_table`.`My_Field` FROM test_table WHERE `test_table`.`My_Field` = 1 ORDER BY `test_table`.`My_Field` ASC GROUP BY `test_table`.`My_Field`"
        self.assertEqual(expected, self.dal.query)


class TestParquetDal(TestCase):

    def setUp(self):
        self.sc = SparkContext._active_spark_context
        self.dirname = os.path.dirname(os.path.abspath(__file__))
        self.dal = ParquetDAL(self.dirname, self.sc)
        self.table_name = "test_table"
        self.filename = "example.parquet"
        self.full_path_file = os.path.join(self.dirname, self.table_name,
                                           self.filename)
        self.dataframe = OrderedDict([('A', [1]), ('B', [2]), ('C', [3])])
        self.df = pd.DataFrame(self.dataframe)
        self.spark_df = self.dal.sql.createDataFrame(self.df,
                                                     self.dataframe.keys())

        self.spark_df.write.parquet(self.full_path_file)

    def test_should_get_schema_from_parquet(self):

        table = ParquetTable(self.table_name, schema_index_file=self.filename)
        self.dal.set_table(table.name, table)
        self.assertEqual(table.schema(), self.dataframe.keys())

    @patch('microdrill.dal.SQLContext.read')
    def test_should_not_connect_twice_on_next_get_schema_from_parquet(self,
                                                                      mock_df):
        dal = ParquetDAL(self.dirname, self.sc)
        table = ParquetTable(self.table_name, schema_index_file=self.filename)
        dal.set_table(table.name, table)  # here get schema too

        self.assertTrue(mock_df.parquet.called)
        mock_df.reset_mock()
        table.schema()
        self.assertFalse(mock_df.parquet.called)

    @patch('microdrill.dal.SQLContext.read')
    def test_should_connect(self, mock_df):
        dal = ParquetDAL(self.dirname, self.sc)
        table = ParquetTable(self.table_name, schema_index_file=self.filename)
        dal.set_table(table.name, table)
        mock_df.reset_mock()
        dal.connect(table.name)
        self.assertTrue(mock_df.parquet.called)

    def test_should_connect_and_execute_query(self):
        table = ParquetTable(self.table_name, schema_index_file=self.filename)
        self.dal.set_table(table.name, table)

        result = self.dal.select(table('A')).execute()

        self.assertIsInstance(result, dataframe.DataFrame)
        self.assertEqual({'A': 1}, result.head().asDict())

    def test_should_connect_and_execute_query_with_multiple_fields(self):
        table = ParquetTable(self.table_name, schema_index_file=self.filename)
        self.dal.set_table(table.name, table)

        result = self.dal.select(table('A'), table('B'), table('C')).execute()

        self.assertDictEqual({'A': 1, 'B': 2, 'C': 3},
                             result.head().asDict())

    def tearDown(self):
        shutil.rmtree(self.full_path_file)
