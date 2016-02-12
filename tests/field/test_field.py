from unittest import TestCase
from microdrill.field import BaseField
from microdrill.table import BaseTable


class TestField(TestCase):

    def setUp(self):
        table = BaseTable('my_table')
        self.field = BaseField('my_field', table)

    def test_should_set_field_name(self):
        self.assertEqual(self.field.name, 'my_field')

    def test_quote_int_value(self):
        self.assertEqual(self.field._quote(2), 2)

    def test_quote_string_value(self):
        self.assertEqual(self.field._quote('bad guy'), "'bad guy'")


class TestQueryField(TestCase):

    def setUp(self):
        table = BaseTable('my_table')
        self.field = BaseField('my_field', table)

    def test_should_return_equal_query(self):
        compare = self.field == 2
        self.assertEqual(compare.query, '`my_table`.`my_field` = 2')

    def test_should_return_not_equal_query(self):
        compare = self.field != 2
        self.assertEqual(compare.query, '`my_table`.`my_field` <> 2')

    def test_should_return_greater_than_query(self):
        compare = self.field > 2
        self.assertEqual(compare.query, '`my_table`.`my_field` > 2')

    def test_should_return_greater_or_equal_than_query(self):
        compare = self.field >= 2
        self.assertEqual(compare.query, '`my_table`.`my_field` >= 2')

    def test_should_return_lower_than_query(self):
        compare = self.field < 2
        self.assertEqual(compare.query, '`my_table`.`my_field` < 2')

    def test_should_return_lower_or_equal_than_query(self):
        compare = self.field <= 2
        self.assertEqual(compare.query, '`my_table`.`my_field` <= 2')

    def test_should_return_regexp_query(self):
        compare = self.field.regexp('a.*')
        self.assertEqual(compare.query, "`my_table`.`my_field` REGEXP 'a.*'")
