#! /usr/bin/env python
# -*- coding: UTF-8 -*- #

from table import ParquetTable

__all__ = ['ParquetPool']


class BasePool(dict):
    def __setitem__(self, key, item):
        self.validate(item)
        super(BasePool, self).__setitem__(key, item)

    def validate(self, tem):
        raise NotImplementedError()


class ParquetPool(BasePool):
    def validate(self, item):
        if not isinstance(item, ParquetTable):
            raise ValueError("Wrong table type")
