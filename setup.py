#! /usr/bin/env python
# -*- coding: UTF-8 -*- #

from setuptools import setup, find_packages

setup(
    name='microdrill',
    version='0.0.3',
    description="Simple Apache Drill alternative using PySpark",
    long_description=open('README.rst').read(),
    keywords='apache drill client parquet hbase pyspark',
    author=u'Globo.com',
    author_email='backstage7@corp.globo.com',
    url='https://github.com/globocom/MicroDrill',
    license='Apache',
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2.7',
        ],
    packages=find_packages(
        exclude=(
            'tests',
        ),
    ),
    include_package_data=True
)
