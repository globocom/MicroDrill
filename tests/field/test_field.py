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
        field1 = ~self.field
        compare = field1 == 2
        self.assertIs(compare.fields[0], field1)

    def test_should_return_sql(self):
        self.assertEqual('`my_table`.`my_field`', self.field.sql())

    def test_should_return_sql_with_sql_template(self):
        self.field.sql_template = '%s ASC'
        self.assertEqual('`my_table`.`my_field` ASC', self.field.sql())

    def test_should_return_sql_with_extra_template(self):
        self.assertEqual('`my_table`.`my_field` ASC', self.field.sql('%s ASC'))

    def test_should_return_sql_with_sql_template_and_extra_template(self):
        self.field.sql_template = '%s ASC'
        self.assertEqual('`my_table`.`my_field` ASC FOOL', self.field.sql('%s FOOL'))

    def test_should_create_new_field_with_count(self):
        field1 = self.field.count

        self.assertNotEqual(id(self.field), id(field1))
        self.assertEqual('COUNT(`my_table`.`my_field`)', field1.sql())

    def test_should_create_new_field_with_avg(self):
        field1 = self.field.avg

        self.assertNotEqual(id(self.field), id(field1))
        self.assertEqual('AVG(`my_table`.`my_field`)', field1.sql())

    def test_should_create_new_field_with_sum(self):
        field1 = self.field.sum

        self.assertNotEqual(id(self.field), id(field1))
        self.assertEqual('SUM(`my_table`.`my_field`)', field1.sql())


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
        field1 = ~self.field

        self.assertNotEqual(id(self.field), id(field1))
        self.assertTrue(field1.invert)

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
