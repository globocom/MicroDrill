import os
import shutil
import pandas as pd
from pyspark import SparkContext
from collections import OrderedDict
from mock import patch
from unittest import TestCase
from tests.helper import FakeTable
from microdrill.table import ParquetTable
from microdrill.dal import BaseDAL, ParquetDAL
from microdrill.field import BaseField
from microdrill.query import BaseQuery


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

    def test_should_return_query_for_select_field(self):
        name = 'My_Field'
        field = BaseField(name, self.table)
        self.table._fields[name] = field
        self.dal.set_table(self.table.name, self.table)
        self.dal.select(field)
        self.assertEqual(self.dal.query,
                         "SELECT `test_table`.`My_Field` FROM test_table")

    def test_should_return_base_query_for_select_field(self):
        name = 'My_Field'
        field = BaseField(name, self.table)
        self.table._fields[name] = field
        self.dal.set_table(self.table.name, self.table)
        self.dal.select(field)
        self.assertIsInstance(self.dal.base_query, BaseQuery)

    def test_should_return_query_for_select_multiple_fields(self):
        name = 'My_Field'
        field = BaseField(name, self.table)
        self.table._fields[name] = field
        name2 = 'My_Field2'
        field2 = BaseField(name2, self.table)
        self.table._fields[name2] = field2
        name3 = 'My_Field3'
        field3 = BaseField(name3, self.table)
        self.table._fields[name3] = field3
        self.dal.set_table(self.table.name, self.table)
        self.dal.select(field, field2, field3)
        expected = "SELECT `test_table`.`My_Field`, `test_table`.`My_Field2`, `test_table`.`My_Field3` FROM test_table"
        self.assertEqual(self.dal.query, expected)

    def test_should_return_dal_when_call_select(self):
        self.assertEqual(id(self.dal), id(self.dal.select()))

    def test_should_return_query_for_where_fields(self):
        name = 'My_Field'
        field = BaseField(name, self.table)
        self.table._fields[name] = field
        self.dal.set_table(self.table.name, self.table)
        self.dal.select(field).where(field==1)
        self.assertEqual(self.dal.query,
                         "SELECT `test_table`.`My_Field` FROM test_table WHERE `test_table`.`My_Field` = 1")

    def test_should_return_query_for_where_multiple_fields(self):
        name = 'My_Field'
        field = BaseField(name, self.table)
        self.table._fields[name] = field
        name2 = 'My_Field2'
        field2 = BaseField(name2, self.table)
        self.table._fields[name2] = field2
        name3 = 'My_Field3'
        field3 = BaseField(name3, self.table)
        self.table._fields[name3] = field3
        self.dal.set_table(self.table.name, self.table)
        self.dal.select(field).where(~(field2 == 2) &(field3 != 1))
        expected = "SELECT `test_table`.`My_Field` FROM test_table WHERE (NOT (`test_table`.`My_Field2` = 2)) AND (`test_table`.`My_Field3` <> 1)"
        self.assertEqual(self.dal.query, expected)

    def test_should_return_dal_when_call_where(self):
        self.assertEqual(id(self.dal), id(self.dal.where()))

    def test_should_return_dal_when_call_order_by(self):
        self.assertEqual(id(self.dal), id(self.dal.order_by()))

    def test_should_return_query_for_order_by_fields(self):
        name = 'My_Field'
        field = BaseField(name, self.table)
        self.table._fields[name] = field
        self.dal.set_table(self.table.name, self.table)
        self.dal.select(field).order_by(field)
        self.assertEqual(self.dal.query,
                         "SELECT `test_table`.`My_Field` FROM test_table ORDER BY `test_table`.`My_Field` ASC")

    def test_should_return_query_for_order_by_multiple_fields(self):
        name = 'My_Field'
        field = BaseField(name, self.table)
        self.table._fields[name] = field
        name2 = 'My_Field2'
        field2 = BaseField(name2, self.table)
        self.table._fields[name2] = field2
        name3 = 'My_Field3'
        field3 = BaseField(name3, self.table)
        self.table._fields[name3] = field3
        self.dal.set_table(self.table.name, self.table)
        self.dal.select(field).order_by(~field2, field3)
        expected = "SELECT `test_table`.`My_Field` FROM test_table ORDER BY `test_table`.`My_Field2` DESC, `test_table`.`My_Field3` ASC"
        self.assertEqual(self.dal.query, expected)

    def test_should_return_dal_when_call_group_by(self):
        self.assertEqual(id(self.dal), id(self.dal.group_by()))

    def test_should_return_query_for_group_by_fields(self):
        name = 'My_Field'
        field = BaseField(name, self.table)
        self.table._fields[name] = field
        self.dal.set_table(self.table.name, self.table)
        self.dal.select(field).group_by(field)
        self.assertEqual(self.dal.query,
                         "SELECT `test_table`.`My_Field` FROM test_table GROUP BY `test_table`.`My_Field`")

    def test_should_return_query_for_group_by_multiple_fields(self):
        name = 'My_Field'
        field = BaseField(name, self.table)
        self.table._fields[name] = field
        name2 = 'My_Field2'
        field2 = BaseField(name2, self.table)
        self.table._fields[name2] = field2
        name3 = 'My_Field3'
        field3 = BaseField(name3, self.table)
        self.table._fields[name3] = field3
        self.dal.set_table(self.table.name, self.table)
        self.dal.select(field).group_by(~field2, field3)
        expected = "SELECT `test_table`.`My_Field` FROM test_table GROUP BY `test_table`.`My_Field2`, `test_table`.`My_Field3`"
        self.assertEqual(self.dal.query, expected)

    def test_should_append_table_in_from_using_group_by(self):
        name = 'My_Field'
        field = BaseField(name, self.table)
        table = FakeTable('test_table2')
        name2 = 'My_Field2'
        field2 = BaseField(name2, table)
        self.table._fields[name] = field
        self.dal.set_table(self.table.name, self.table)
        self.dal.select(field).group_by(field2)

        self.assertEqual(2, self.dal.query.count('test_table2'))

    def test_should_append_table_in_from_using_where(self):
        name = 'My_Field'
        field = BaseField(name, self.table)
        table = FakeTable('test_table2')
        name2 = 'My_Field2'
        field2 = BaseField(name2, table)
        self.table._fields[name] = field
        self.dal.set_table(self.table.name, self.table)
        self.dal.select(field).where(field2 == 2)

        self.assertEqual(2, self.dal.query.count('test_table2'))


class TestParquetDal(TestCase):

    def setUp(self):
        self.sc = SparkContext._active_spark_context
        self.dirname = os.path.dirname(os.path.abspath(__file__))
        self.dal = ParquetDAL(self.dirname, self.sc)
        self.filename = "example.parquet"
        self.full_path_file = os.path.join(self.dirname, self.filename)
        self.dataframe = OrderedDict([('A', [1]), ('B', [2]), ('C', [3])])
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

    def test_should_connect_and_execute_query(self):
        table = ParquetTable('test_table', schema_index_file=self.filename)
        self.dal.set_table(table.name, table)

        result = self.dal.select(table('A')).execute()

        self.assertIsInstance(result, pd.core.frame.DataFrame)
        self.assertEqual(1, result.values[0][0])

    def test_should_connect_and_execute_query_with_multiple_fields(self):
        table = ParquetTable('test_table', schema_index_file=self.filename)
        self.dal.set_table(table.name, table)

        result = self.dal.select(table('A'), table('B'), table('C')).execute()

        self.assertEqual(1, result.values[0][0])
        self.assertEqual(2, result.values[0][1])
        self.assertEqual(3, result.values[0][2])

    def tearDown(self):
        shutil.rmtree(self.full_path_file)
