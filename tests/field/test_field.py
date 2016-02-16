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

    def test_should_decorate_adding_not_for_field_with_invert(self):
        self.field._invert = True
        base_query = (self.field == 2)
        self.assertEqual(base_query.query, 'NOT (`my_table`.`my_field` = 2)')

    def test_should_not_decorate_adding_not_for_field_without_invert(self):
        base_query = (self.field == 2)
        self.field._check_and_do_invert(base_query)
        self.assertEqual(base_query.query, '`my_table`.`my_field` = 2')

    def test_should_return_base_query_with_field(self):
        compare = self.field == 2
        self.assertIs(compare.fields[0], self.field)

    def test_should_return_base_query_with_not_field(self):
        compare = ~self.field == 2
        self.assertIs(compare.fields[0], self.field)


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

    def test_should_invert_field(self):
        ~self.field
        self.assertTrue(self.field.invert)

    def test_should_not_invert_field(self):
        self.assertFalse(self.field.invert)

    def test_should_return_regexp_query(self):
        compare = self.field.regexp('a.*')
        self.assertEqual(compare.query, "`my_table`.`my_field` REGEXP 'a.*'")

    def test_should_return_regexp_not_query(self):
        compare = ~self.field.regexp('a.*')
        self.assertEqual(compare.query, "NOT (`my_table`.`my_field` REGEXP 'a.*')")

    def test_should_return_equal_not_query(self):
        compare = ~self.field == 2
        self.assertEqual(compare.query, "NOT (`my_table`.`my_field` = 2)")
