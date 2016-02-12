from unittest import TestCase
from microdrill.field import BaseField
from microdrill.table import BaseTable


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
