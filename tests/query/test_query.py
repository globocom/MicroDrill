from unittest import TestCase
from microdrill.field import BaseField
from microdrill.table import BaseTable
from microdrill.query import BaseQuery


class TestQuery(TestCase):

    def setUp(self):
        table = BaseTable('my_table')
        self.field = BaseField('my_field', table)

    def test_should_return_equal_and_equal_query(self):
        compare = (self.field == 2) & (self.field == 'new value')
        self.assertEqual(
            compare.query,
            "(`my_table`.`my_field` = 2) AND (`my_table`.`my_field` = 'new value')"
        )

    def test_should_return_equal_or_equal_query(self):
        compare = (self.field == 2) | (self.field == 'new value')
        self.assertEqual(
            compare.query,
            "(`my_table`.`my_field` = 2) OR (`my_table`.`my_field` = 'new value')"
        )

    def test_should_return_equal_or_not_equal_query(self):
        compare = (self.field == 2) | ~(self.field == 'new value')
        self.assertEqual(
            compare.query,
            "(`my_table`.`my_field` = 2) OR (NOT (`my_table`.`my_field` = 'new value'))"
        )

    def test_should_return_query_added_with_string(self):
        compare = (self.field == 2) | ~(self.field == 'new value')
        compare = compare + "New String"
        self.assertEqual(
            compare.query,
            "(`my_table`.`my_field` = 2) OR (NOT (`my_table`.`my_field` = 'new value')) New String"
        )

    def test_should_return_query_added_with_base_query(self):
        compare = (self.field == 2) | ~(self.field == 'new value')
        compare = compare + BaseQuery("New String")
        self.assertEqual(
            compare.query,
            "(`my_table`.`my_field` = 2) OR (NOT (`my_table`.`my_field` = 'new value')) New String"
        )

    def test_should_return_equal_and_equal_with_different_fields(self):
        table = BaseTable('my_table2')
        field = BaseField('my_field2', table)
        compare = (self.field == 2) & (field == 'new value')
        self.assertListEqual(
            compare.fields,
            [self.field, field]
        )
