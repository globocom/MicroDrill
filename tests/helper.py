from microdrill.table import BaseTable
from microdrill.pool import BasePool
from microdrill.dal import BaseDAL
from microdrill.field import BaseField


class FakeTable(BaseTable):
    pass


class FakePool(BasePool):

    def validate(self, item):
        pass


class FakeDAL(BaseDAL):
    def __init__(self):
        super(FakeDAL, self).__init__()
        self._tables = FakePool()


def factory_dals(dals):
    list_dals = []

    for dal in dals:
        current_dal = FakeDAL()
        table_name = dal['tables']['name']
        table_uri = dal['tables']['uri']

        table = FakeTable(table_name, table_uri)
        current_dal.set_table(table)
        list_dals.append(current_dal)

    return list_dals


def factory_field(table):
    name = 'My_Field'
    if table._fields:
        name += '%s' % len(table._fields)

    field = BaseField(name, table)
    table._fields[name] = field
    return field


def get_table_from_proxy(proxy, index_dal, table_name):
    return proxy._dals[index_dal].tables[table_name]
