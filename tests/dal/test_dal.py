import os
import shutil
import pandas as pd
from mock import patch
from unittest import TestCase
from microdrill.database import BaseDatabase, ParquetDatabase
from tests.helper import FakeDatabase
from microdrill.dal import BaseDAL, ParquetDAL
from pyspark import SparkContext


class TestBaseDal(TestCase):

    def setUp(self):
        self.dal = BaseDAL()

    def test_should_set_database_in_dal(self):
        database = FakeDatabase('Test Database', 'hdfs://...')
        self.dal.set_database(database.name, database)
        self.assertEqual(self.dal.databases.get(database.name), database)
        self.assertEqual(len(self.dal.databases), 1)

    def test_should_overwrite_database_in_dal_with_same_name(self):
        database = FakeDatabase('Test Database', 'hdfs://...')
        database2 = FakeDatabase('Test Database', 'hdfs://...2')
        self.dal.set_database(database.name, database)
        self.dal.set_database(database2.name, database2)
        self.assertEqual(self.dal.databases.get(database.name), database2)
        self.assertEqual(len(self.dal.databases), 1)

    def test_should_set_multiple_databases_in_dal(self):
        database = FakeDatabase('Test Database', 'hdfs://...')
        database2 = FakeDatabase('Test Database2', 'hdfs://...2')
        database3 = FakeDatabase('Test Database3', 'hdfs://...3')
        self.dal.set_database(database.name, database)
        self.dal.set_database(database2.name, database2)
        self.dal.set_database(database3.name, database3)
        self.assertEqual(self.dal.databases.get(database.name), database)
        self.assertEqual(self.dal.databases.get(database2.name), database2)
        self.assertEqual(self.dal.databases.get(database3.name), database3)
        self.assertEqual(len(self.dal.databases), 3)

    def test_should_configure_correct_database_by_name(self):
        database = FakeDatabase('Test Database', 'hdfs://...')
        database2 = FakeDatabase('Test Database2', 'hdfs://...2')
        self.dal.set_database(database.name, database)
        self.dal.set_database(database2.name, database2)
        self.dal.configure(database.name, make_happy='Ok')
        self.assertEqual(database.config.get('make_happy'), 'Ok')
        self.assertEqual(database2.config.get('make_happy'), None)

    def test_should_overwrite_configuration_when_configure_called(self):
        database = FakeDatabase('Test Database', 'hdfs://...')
        self.dal.set_database(database.name, database)
        self.dal.configure(database.name, make_sad=':(')
        self.dal.configure(database.name, make_happy=':)')
        self.assertEqual(database.config.get('make_happy'), ':)')
        self.assertEqual(database.config.get('make_sad'), None)


class TestParquetDal(TestCase):

    def setUp(self):
        self.sc = SparkContext._active_spark_context
        self.dal = ParquetDAL(self.sc)
        self.filename = "example.parquet"
        self.dirname = os.path.dirname(os.path.abspath(__file__))
        self.full_path_file = os.path.join(self.dirname, self.filename)
        self.dataframe = {'A': [1], 'B': [2], 'C': [3]}
        self.df = pd.DataFrame(self.dataframe)
        self.spark_df = self.dal.sql.createDataFrame(self.df, self.dataframe.keys())

        self.spark_df.write.parquet(self.full_path_file)

    def test_should_get_schema_from_parquet(self):

        database = ParquetDatabase('Test database', self.dirname,
                                   schema_index_file=self.filename)
        self.dal.set_database(database.name, database)
        self.assertEqual(self.dal.schema(database.name), self.dataframe.keys())

    @patch('microdrill.dal.SQLContext.read')
    def test_should_not_connect_twice_on_next_get_schema_from_parquet(self, mock_df):
        dal = ParquetDAL(self.sc)
        database = ParquetDatabase('Test database', self.dirname,
                                   schema_index_file=self.filename)
        dal.set_database(database.name, database)
        dal.schema(database.name)

        self.assertTrue(mock_df.parquet.called)
        mock_df.reset_mock()
        dal.schema(database.name)
        self.assertFalse(mock_df.parquet.called)

    @patch('microdrill.dal.SQLContext.read')
    def test_should_not_connect_without_configure(self, mock_df):
        dal = ParquetDAL(self.sc)
        database = ParquetDatabase('Test database', self.dirname,
                                   schema_index_file=self.filename)
        dal.set_database(database.name, database)

        dal.connect(database.name)
        self.assertFalse(mock_df.parquet.called)

    @patch('microdrill.dal.SQLContext.read')
    def test_should_connect_with_configure(self, mock_df):
        dal = ParquetDAL(self.sc)
        database = ParquetDatabase('Test database', self.dirname,
                                   schema_index_file=self.filename)
        dal.set_database(database.name, database)

        dal.configure(database.name, files=[self.filename])
        dal.connect(database.name)
        self.assertTrue(mock_df.parquet.called)

    @patch('microdrill.dal.SQLContext.read')
    def test_should_overwrite_connection_with_configure(self, mock_df):
        dal = ParquetDAL(self.sc)
        database = ParquetDatabase('Test database', self.dirname,
                                   schema_index_file=self.filename)
        dal.set_database(database.name, database)

        dal.configure(database.name, files=[self.filename])
        dal.connect(database.name)
        self.assertTrue(mock_df.parquet.called)

        mock_df.reset_mock()
        dal.connect(database.name)
        self.assertTrue(mock_df.parquet.called)

    def tearDown(self):
        shutil.rmtree(self.full_path_file)
