from unittest import TestCase

from mock import patch, call

from microdrill.proxy import ProxyDAL
from tests.helper import factory_dals, get_database_from_proxy


class TestProxy(TestCase):

    DALs = [{'databases': {'name': 'database1', 'uri': 'uri'}},
            {'databases': {'name': 'database2', 'uri': 'uri'}}]

    def setUp(self):
        self.proxy = ProxyDAL(factory_dals(self.DALs))

    def test_should_return_all_databases(self):
        self.assertListEqual(['database1', 'database2'], self.proxy.databases)

    @patch('tests.helper.FakeDAL.schema', return_value=['col1', 'col2'])
    def test_should_return_schema(self, mock):
        proxy = ProxyDAL(factory_dals(self.DALs))
        self.assertListEqual(['col1', 'col2'], proxy.schema('name'))

    @patch('tests.helper.FakeDAL.schema', return_value=[])
    def test_should_return_exception_when_schema_not_found(self, mock):
        proxy = ProxyDAL(factory_dals(self.DALs))
        with self.assertRaises(ValueError):
            proxy.schema('name')

    def test_should_return_empty_list_when_name_empty(self):
        self.assertEqual([], self.proxy.schema(None))

    def test_should_configure_dal(self):
        self.proxy.configure_connection('database1', files='test1')
        self.proxy.configure_connection('database2', files='test2',
                                        pool_size=3)

        database1 = get_database_from_proxy(self.proxy, 0, 'database1')
        self.assertEqual('test1', database1.config.get('files'))
        self.assertEqual(None, database1.config.get('pool_size'))

        database2 = get_database_from_proxy(self.proxy, 1, 'database2')
        self.assertEqual('test2', database2.config.get('files'))
        self.assertEqual(3, database2.config.get('pool_size'))

    @patch('tests.helper.FakeDAL.connect')
    def test_should_connect(self, mock):
        proxy = ProxyDAL(factory_dals(self.DALs))
        proxy.connect('name')
        mock.assert_has_calls([call('name')])
        self.assertEqual(2, mock.call_count)
