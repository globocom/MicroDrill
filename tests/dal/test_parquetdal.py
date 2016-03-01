#! /usr/bin/env python
# -*- coding: UTF-8 -*- #

import os
import shutil
from collections import OrderedDict
from unittest import TestCase

import pandas as pd
from mock import patch
from pyspark import SparkContext
from pyspark.sql import dataframe

from microdrill.dal.parquet import ParquetDAL
from microdrill.table import ParquetTable


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
        self.spark_df = self.dal.context.createDataFrame(self.df,
                                                         self.dataframe.keys())

        self.spark_df.write.parquet(self.full_path_file)

    def test_should_get_schema_from_parquet(self):

        table = ParquetTable(self.table_name, schema_index_file=self.filename)
        self.dal.set_table(table)
        self.assertEqual(table.schema(), self.dataframe.keys())

    def test_should_get_schema_from_parquet_with_schema_setter(self):

        table = ParquetTable(self.table_name)
        table.schema_index_file = self.filename
        self.dal.set_table(table)
        self.assertEqual(table.schema(), self.dataframe.keys())

    @patch('microdrill.dal.parquet.SQLContext.read')
    def test_should_not_connect_twice_on_next_get_schema_from_parquet(self,
                                                                      mock_df):
        dal = ParquetDAL(self.dirname, self.sc)
        table = ParquetTable(self.table_name, schema_index_file=self.filename)
        dal.set_table(table)  # here it gets schema too

        self.assertTrue(mock_df.parquet.called)
        mock_df.reset_mock()
        table.schema()
        self.assertFalse(mock_df.parquet.called)

    @patch('microdrill.dal.parquet.SQLContext.read')
    def test_should_connect(self, mock_df):
        dal = ParquetDAL(self.dirname, self.sc)
        table = ParquetTable(self.table_name, schema_index_file=self.filename)
        dal.set_table(table)
        mock_df.reset_mock()
        dal.connect(table.name)
        self.assertTrue(mock_df.parquet.called)

    @patch('microdrill.dal.parquet.SQLContext.read')
    def test_should_raise_error_connecting_to_not_found_table(self, mock_df):
        dal = ParquetDAL(self.dirname, self.sc)
        table = ParquetTable(self.table_name, schema_index_file=self.filename)
        dal.set_table(table)
        self.assertRaises(ValueError, dal.connect, 'not_found_table')

    def test_should_connect_and_execute_query(self):
        table = ParquetTable(self.table_name, schema_index_file=self.filename)
        self.dal.set_table(table)

        result = self.dal.select(table('A')).execute()

        self.assertIsInstance(result, dataframe.DataFrame)
        self.assertEqual({'A': 1}, result.head().asDict())

    def test_should_connect_and_execute_query_with_multiple_fields(self):
        table = ParquetTable(self.table_name, schema_index_file=self.filename)
        self.dal.set_table(table)

        result = self.dal.select(table('A'), table('B'), table('C')).execute()

        self.assertDictEqual({'A': 1, 'B': 2, 'C': 3},
                             result.head().asDict())

    def test_should_reset_query_when_execute(self):
        table = ParquetTable(self.table_name, schema_index_file=self.filename)
        self.dal.set_table(table)

        self.dal.select(table('A'), table('B'), table('C'))
        query_select = self.dal._query['select']
        self.dal.execute()

        self.assertNotEqual(id(query_select), id(self.dal._query.get('select')))

    def tearDown(self):
        shutil.rmtree(self.full_path_file)
