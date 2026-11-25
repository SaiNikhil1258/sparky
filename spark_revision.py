"""
===========================================================
                    SPARK BOILERPLATE
===========================================================

Purpose:
Initialize SparkContext and SparkSession.

SparkContext:
    Entry point for working with RDDs.

SparkSession:
    Entry point for working with DataFrames and Spark SQL.
"""

import os
import sys
import urllib.request

from collections import namedtuple
from datetime import datetime

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Optional: Configure the Python interpreter used by PySpark workers.
# This is useful when the driver and worker Python environments differ.

# python_path = sys.executable
# os.environ['PYSPARK_PYTHON'] = python_path

# Optional: Specify JAVA_HOME if Spark needs a particular Java version.
# os.environ['JAVA_HOME'] = r'C:\Users\LENOVO\.jdks\corretto-1.8.0_422'


# Spark Configuration
conf = (
    SparkConf()
    .setAppName("pyspark")
    .setMaster("local[*]")  # Run Spark locally using all available cores.
    .set("spark.driver.host", "localhost")
    .set("spark.default.parallelism", "1")
)

# Initialize SparkContext.
# Used for low-level RDD operations.
sc = SparkContext(conf=conf)

# Initialize SparkSession.
# Used for DataFrame and SQL operations.
spark = SparkSession.builder.getOrCreate()


"""
===========================================================
                1. RDD CREATION AND MAP()
===========================================================

Objective:
    Create a Python list and convert it into an RDD.
    Add 2 to every element.

Concepts:
    sc.parallelize() -> Converts a Python collection into an RDD.
    map()            -> Applies a function to every element.
    collect()        -> Brings all RDD elements back to the driver.

Remember:
    map() is a transformation.
    collect() is an action.
"""

arr = [1, 4, 6, 7]

print("\033[93mOriginal list\033[0m\n", arr)

# Convert Python list into an RDD.
list_rdd = sc.parallelize(arr)

# Add 2 to every element using map().
addition_rdd = list_rdd.map(lambda x: x + 2)

# collect() triggers execution and returns the result to the driver.
print("\033[93mRDD after addition of 2\033[0m\n", addition_rdd.collect())


"""
===========================================================
                2. RDD FILTER OPERATION
===========================================================

Objective:
    Filter strings that contain the substring "Nik".

Concepts:
    filter() -> Retains elements satisfying a condition.
    in       -> Checks whether a substring exists inside a string.

Remember:
    filter() is a transformation.
    It does not execute immediately because Spark uses lazy evaluation.
"""

str_arr = ["Nikhil", "Nik", "Nike", "Pyspark"]

print("\033[93mThe string list is\033[0m\n", str_arr)

str_rdd = sc.parallelize(str_arr)

# Retain only strings containing "Nik".
filtered_rdd = str_rdd.filter(lambda x: "Nik" in x)

print("\033[93mRDD after filtering\033[0m\n", filtered_rdd.collect())


"""
===========================================================
                3. READ A TEXT FILE AS RDD
===========================================================

Objective:
    Read file1.txt and retrieve records containing "Gymnastics".

Concepts:
    textFile() -> Reads a text file and creates an RDD.
    filter()   -> Filters records based on a condition.
    take(n)    -> Returns the first n elements to the driver.

Remember:
    textFile() creates an RDD where each record represents
    one line of the input file.

    The second argument specifies the minimum number of
    partitions, not necessarily the final partition count.
"""

print("\033[93mReading file1 as RDD and filtering Gymnastics\033[0m\n")

file1 = sc.textFile("./data/file1.txt", 1)

# Filter records containing the word Gymnastics.
gymData_file1_rdd = file1.filter(lambda x: "Gymnastics" in x)

# take(5) retrieves only five records.
# The * operator unpacks the list returned by take().
# sep='\n' prints every record on a separate line.

print("\033[93mFirst five matching records\033[0m")

print(*gymData_file1_rdd.take(5), sep="\n")


"""
===========================================================
        4. CREATE A STRUCTURED RDD USING NAMED TUPLE
===========================================================

Objective:
    Split each CSV record into individual fields.
    Assign meaningful column names using namedtuple.
    Filter records where the PRODUCT contains Gymnastics.

Why namedtuple?

    Initially, each CSV record is just a string.

    After splitting, we have a collection of values.

    namedtuple allows us to access those values using
    meaningful field names instead of numeric indexes.

    Example:
        x[0]       -> Transaction number
        x[1]       -> Transaction date
        x.product  -> Product name

Concepts:
    split()       -> Separates CSV fields.
    namedtuple()  -> Creates a tuple with named fields.
    map()         -> Converts every record into a named tuple.
    filter()      -> Filters records using named attributes.
"""

print("\n\033[93mCreating a structured RDD using namedtuple\033[0m")

# Define the schema/field names for each transaction.
named_tuple = namedtuple(
    "named_tuple",
    [
        "txnno",
        "txndate",
        "custno",
        "amount",
        "category",
        "product",
        "city",
        "state",
        "spendby",
    ],
)

# Read the source file as an RDD.
product_rdd_raw = sc.textFile("./data/file1.txt")

# Split every CSV record using comma as the delimiter.
split_rdd = product_rdd_raw.map(lambda x: x.split(","))

# Convert each split record into a named tuple.
schema_rdd = split_rdd.map(
    lambda x: named_tuple(x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8])
)

# Filter records where the PRODUCT column contains Gymnastics.
gymdata_product_rdd = schema_rdd.filter(lambda x: "Gymnastics" in x.product)

# Print the first five matching named tuples.
print(*gymdata_product_rdd.take(5), sep="\n")


"""
===========================================================
                5. CONVERT RDD INTO DATAFRAME
===========================================================

Objective:
    Convert the structured RDD into a Spark DataFrame.

Why do this?

    RDDs provide low-level distributed data processing.

    DataFrames provide a structured, schema-based API.

    DataFrames allow us to use:
        - SQL queries
        - Column operations
        - GroupBy and aggregations
        - Optimized execution through Catalyst

Because we used namedtuple, Spark can infer the column names.
"""

print("\n\033[93mConverting RDD into DataFrame\033[0m")

schemadf = gymdata_product_rdd.toDF()

schemadf.show(5)


"""
===========================================================
                6. READ CSV FILE AS DATAFRAME
===========================================================

Objective:
    Read file3.txt as a CSV DataFrame.

Concepts:
    spark.read       -> DataFrameReader API.
    format("csv")    -> Specifies the input format.
    header=True      -> Treats the first row as column names.
    load()           -> Loads the source data.

Remember:
    Without header=True, Spark treats the first row as data
    and generates default column names such as _c0, _c1, etc.
"""

print("\n\033[93mReading CSV file with header=True\033[0m")

file3df = spark.read.format("csv").options(header=True).load("./data/file3.txt")

file3df.show(5)


"""
===========================================================
                7. READ JSON AND PARQUET FILES
===========================================================

Objective:
    Read two different file formats into DataFrames.

JSON:
    JSON is a semi-structured data format.
    Spark can infer the schema from the JSON records.

PARQUET:
    Parquet is a columnar storage format.
    It is optimized for analytical workloads.

Remember:
    spark.read.load() uses Parquet as its default format
    when no format is specified.
"""

print("\n\033[93mReading JSON and Parquet files\033[0m")

# Read JSON file.
json_df = spark.read.format("json").load("./data/file4.json")

json_df.show(5)


# Read Parquet file.
# Parquet is the default format for spark.read.load().
parquet_df = spark.read.load("./data/file5.parquet")

parquet_df.show(5)


"""
===========================================================
          8. SCHEMA ALIGNMENT AND UNION OF DATAFRAMES
===========================================================

Objective:
    Combine three DataFrames into one DataFrame.

Problem:
    The DataFrames contain the same columns, but their
    column positions are different.

    Example:

    DataFrame 1:
        txnno, txndate, custno, amount, category...

    DataFrame 2:
        amount, category, city, custno, product...

    DataFrame 3:
        txnno, txndate, custno, amount, category...

Solution:
    1. Get the column order from schemadf.
    2. Select the same columns in the same order
       from json_df and parquet_df.
    3. Perform union().

Important:
    union() combines rows based on column POSITION,
    not column NAME.

    unionByName() is an alternative that aligns columns
    based on their names.

Remember:
    union() does not remove duplicate records.
    union() behaves like UNION ALL in SQL.
"""

print("\n\033[93mSchema alignment and DataFrame union\033[0m")

# Get the column order from the first DataFrame.
column_headers = schemadf.columns

# Reorder the JSON DataFrame columns.
json_df_final = json_df.select(column_headers)

# Reorder the Parquet DataFrame columns.
parquet_df_final = parquet_df.select(column_headers)

# Combine all three DataFrames vertically.
uniondf = schemadf.union(json_df_final).union(parquet_df_final)

uniondf.show(5)


"""
===========================================================
          9. DATAFRAME TRANSFORMATIONS
===========================================================

Objective:
    Starting from uniondf:

    1. Extract the year from txndate.
    2. Rename txndate to year.
    3. Create a status column.
    4. Filter transactions where txnno > 50000.

Concepts:

    withColumn():
        Creates a new column or replaces an existing column.

    expr():
        Allows us to write Spark SQL expressions inside
        DataFrame transformations.

    split():
        Splits a string using a delimiter.

    withColumnRenamed():
        Renames an existing column.

    CASE WHEN:
        Implements conditional logic.

    filter():
        Retains records matching a condition.
"""

print("\n\033[93mExtract year, create status and filter transactions\033[0m")

year_status_df = (
    uniondf
    # txndate format: MM-DD-YYYY
    # split(txndate, '-')[2] extracts the year.
    .withColumn("txndate", expr("split(txndate, '-')[2]"))
    # Rename txndate to year.
    .withColumnRenamed("txndate", "year")
    # Create a status column:
    # Cash   -> 1
    # Credit -> 0
    .withColumn("status", expr("CASE WHEN spendby = 'cash' THEN 1 ELSE 0 END"))
    # Keep only transactions with transaction number > 50000.
    .filter("txnno > 50000")
)

year_status_df.show(5)


"""
===========================================================
                10. GROUP BY AND AGGREGATION
===========================================================

Objective:
    Calculate the total transaction amount for each category.

Example:

    Category          Amount
    --------------------------------
    Gymnastics         100
    Gymnastics         200
    Team Sports        300
    Team Sports        150

Expected aggregation:

    Category          Total
    --------------------------------
    Gymnastics         300
    Team Sports        450

Concepts:
    groupBy() -> Groups records based on a column.
    agg()     -> Applies aggregation functions.
    sum()     -> Calculates the total amount.

Remember:
    This is equivalent to:

    SELECT category, SUM(amount)
    FROM transactions
    GROUP BY category;
"""

print("\n\033[93mCalculate total amount for each category\033[0m")

goupby_df = year_status_df.groupBy("category").agg(sum("amount").alias("total"))

goupby_df.show(5)


"""
===========================================================
                11. DATA TYPE CASTING
===========================================================

Objective:
    Convert the aggregated total column into IntegerType.

Why?

    The sum operation on amount currently returns a
    floating-point value.

    Example:
        194700.86999999976

    Casting to integer gives:
        194700

Concepts:
    withColumn() -> Replaces an existing column.
    col()        -> References a DataFrame column.
    cast()       -> Converts the data type.

Important:
    Casting to INT truncates the decimal portion.
    It does not round the value.
"""

print("\n\033[93mCasting total column to IntegerType\033[0m")

casting_df = goupby_df.withColumn("total", col("total").cast("int"))

casting_df.show(5)


"""
===========================================================
                    END OF REVISION
===========================================================

Topics covered:

    1.[ ] RDD creation using parallelize()
    2.[ ] RDD transformations using map()
    3.[ ] RDD filtering using filter()
    4.[ ] Reading text files using textFile()
    5.[ ] Creating named tuples
    6.[ ] Converting RDDs into DataFrames
    7.[ ] Reading CSV, JSON and Parquet
    8.[ ] Schema alignment
    9.[ ] Union operations
   10.[ ] DataFrame transformations
   11.[ ] CASE WHEN expressions
   12.[ ] GroupBy and aggregations
   13.[ ] Data type casting
"""

# Optional: Stop SparkContext after all processing is complete.
sc.stop()
