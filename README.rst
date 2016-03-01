MicroDrill
##########
Simple `Apache Drill
<https://drill.apache.org/docs/>`_
alternative using `PySpark
<https://spark.apache.org/docs/1.6.0/api/python/index.html>`_
inspired by `PyDAL
<https://github.com/web2py/pydal>`_


Setup
=====
Run terminal command ``pip install microdrill``


Dependencies
============
PySpark was tested with `Spark 1.6
<https://spark.apache.org/docs/1.6.0/programming-guide.html>`_


Usage
=====

Defining Query Parquet Table
____________________________
``ParquetTable(table_name, schema_index_file=file_name)``

* table_name: Table referenced name.
* file_name: File name to search for table schema.

Using Parquet DAL
_________________
``ParquetDAL(file_uri, sc)``

* file_uri: It can be the path to files or ``hdfs://`` or any other location
* sc: Spark Context (https://spark.apache.org/docs/1.6.0/api/python/pyspark.html#pyspark.SparkContext)

Connecting in tables
_____________________
| ``parquet_conn = ParquetDAL(file_uri, sc)``
| ``parquet_table = ParquetTable(table_name, schema_index_file=file_name)``
| ``parquet_conn.set_table(parquet_table)``

Queries
_______
Returning Table Object
**********************
``parquet_conn(table_name)``

Returning Field Object
**********************
``parquet_conn(table_name)(field_name)``

Basic Query
***********
| ``parquet_conn.select(field_object, [field_object2, ...]).where(field_object=value)``
| ``parquet_conn.select(field_object1, field_object2).where(field_object1==value1 & ~field_object2==value2)``
| ``parquet_conn.select(field_object1, field_object2).where(field_object1!=value1 | field_object1.regexp(reg_exp))``

Grouping By
***********
``parquet_conn.groupby(field_object1, [field_object2, ...])``

Ordering By
***********
| ``parquet_conn.orderby(field_object1, [field_object2, ...])``
| ``parquet_conn.orderby(~field_object)``

Limiting
********
``parquet_conn.limit(number)``

Executing
*********
``df = parquet_conn.execute()``
execute() returns a `PySpark DataFrame.
<https://spark.apache.org/docs/1.6.0/api/python/pyspark.sql.html#pyspark.sql.DataFrame>`_

Returning Field Names From Schema
*********************************
``parquet_conn(table_name).schema()``


Developers
==========
Install latest jdk and run in terminal ``make setup``
