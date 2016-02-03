#! /usr/bin/env python
# -*- coding: UTF-8 -*- #

from database import ParquetDatabase

__all__ = ['ParquetPool']


class BasePool(dict):
    def __setitem__(self, key, item):
        self.validate(item)
        super(BasePool, self).__setitem__(key, item)

    def validate(self, tem):
        raise NotImplementedError()


class ParquetPool(BasePool):
    def validate(self, item):
        if not isinstance(item, ParquetDatabase):
            raise ValueError("Wrong database type")
