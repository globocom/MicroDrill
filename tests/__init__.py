#! /usr/bin/env python
# -*- coding: UTF-8 -*- #

import os
import sys
import warnings

test_dir = os.path.dirname(os.path.abspath(__file__))
spark_home = os.environ.get('SPARK_HOME', None)
if not spark_home:
    warnings.warn('SPARK_HOME environment variable is not set, '
                  'using spark dir inside tests',
                  ImportWarning)
    spark_home = 'spark'
    os.environ['SPARK_HOME'] = os.path.join(test_dir, spark_home)
sys.path.insert(0, os.path.join(test_dir, spark_home, 'python'))
sys.path.insert(0, os.path.join(test_dir, spark_home,
                                'python/lib/py4j-0.9-src.zip'))
execfile(os.path.join(test_dir, spark_home, 'python/pyspark/shell.py'))
