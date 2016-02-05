from microdrill.database import BaseDatabase
from microdrill.pool import BasePool
from microdrill.dal import BaseDAL


class FakeDatabase(BaseDatabase):
    pass


class FakePool(BasePool):

    def validate(self, item):
        pass


class FakeDAL(BaseDAL):
    def __init__(self):
        super(FakeDAL, self).__init__()
        self._databases = FakePool()


def factory_dals(dals):
    list_dals = []

    for dal in dals:
        current_dal = FakeDAL()
        database_name = dal['databases']['name']
        database_uri = dal['databases']['uri']

        database = FakeDatabase(database_name, database_uri)
        current_dal.set_database(database_name, database)
        list_dals.append(current_dal)

    return list_dals


def get_database_from_proxy(proxy, index_dal, database_name):
    return proxy._dals[index_dal].databases[database_name]
