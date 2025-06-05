import os
import urllib.request
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
import sys
from collections import namedtuple
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['JAVA_HOME'] = r'C:\Users\LENOVO\.jdks\corretto-1.8.0_422'

conf = (SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host", "localhost").set(
	"spark.default.parallelism", "1"))
sc = SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()
import urllib.request
from datetime import datetime
from pyspark.sql.window import Window

# exec(urllib.request.urlopen(
# 	"https://gist.githubusercontent.com/saiadityaus1/889aa99339c5d5bc67f96d7420c46923/raw").read().decode('utf-8'))
"""
SPARK BOILERPLATE
"""
# print("\033[91m" + "This is red text" + "\033[0m")

# INFO: 1. create a python list with 1,4,6,8 and convert it to a rdd and then add 2 to each element of the rdd
arr = [1, 4, 6, 7]
print("\033[93m" + "original list" + "\033[0m\n", arr)
list_rdd = sc.parallelize(arr)
addition_rdd = list_rdd.map(lambda x: x + 2)
print("\033[93m" + "RDD after the addition of 2" + "\033[0m\n", addition_rdd.collect())

# INFO: 2. Create a python list with some text and then filter the strings with similar name or containing the string.
str_arr = ["Nikhil", "Nik", "Nike", "Pyspark"]
print("\033[93m" + "The string list is" + "\033[0m\n", str_arr)
str_rdd = sc.parallelize(str_arr)
filtered_rdd = str_rdd.filter(lambda x: "Nik" in x)
print("\033[93m" + "rdd after filtering with in operator" + "\033[0m\n", filtered_rdd.collect())

# INFO: 3. Read the file1 as rdd and filter rows containing "gymnastics"
print("\033[93m" + "Read the file1 as rdd and filter rows containing 'gymnastics'" + "\033[0m\n")
# print("Read the file1 as rdd and filter rows containing 'gymnastics'")
file1 = sc.textFile("file1.txt", 1)
gymData_file1_rdd = file1.filter(lambda x: "Gymnastics" in x)
# gymData_file1_rdd.foreach(print)
print("\033[93m" + "printing only five rows with .take() and sep option" + "\033[0m")
# NOTE: in a rdd if you want to print only some selected top rows to do that you unpack the rdd and then .take(n) and add
# the option seperator as sep=\n
print(*gymData_file1_rdd.take(5), sep='\n')

# INFO:4. Create a named tuple and impose Nmaed tuple to it for schema rdd and filter products contains Gymnastics?
print("\n\033[93m" + "Create a named tuple and impose Nmaed tuple to it for schema rdd and filter products contains Gymnastics?" + "\033[0m")
named_tuple = namedtuple('named_tuple',
						 ["txnno", "txndate", "custno", "amount", "category", "product", "city", "state",
						  "spendby"])
product_rdd_raw =  sc.textFile("file1.txt")
split_rdd = file1.map(lambda x : x.split(","))
schema_rdd = split_rdd.map(lambda x : named_tuple(x[0], x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8]))
gymdata_product_rdd = schema_rdd.filter(lambda x : "Gymnastics" in x.product)
# gymdata_product_rdd.foreach(print)
print(*gymdata_product_rdd.take(5), sep='\n')

# INFO: 5. Convert this rdd into a df
print("\n\033[93m" + "converting the RDD to DF with .toDF() method" + "\033[0m")
schemadf = gymdata_product_rdd.toDF()
schemadf.show(5)


# INFO: Read file3 as csv with header True
print("\n\033[93m" + "Reading a file with header options as true for the DF" + "\033[0m")
file3df = spark.read.format("csv").options(header=True).load("file3.txt")
file3df.show(5)

# INFO: read file4 as json and file5 as parquet and show the df
print("\n\033[93m" + "read file4 as json and file5 as parquet and show the df" + "\033[0m")
json_df = spark.read.format("json").load("file4.json")
json_df.show(5)
# NOTE: for the file format parquet we need  not specify the format of the file since parquet is the default file format in Spark
parquet_df = spark.read.load("file5.parquet")
parquet_df.show(5)

# INFO: Define a unified columns list and impost using selecct for all the dataframes and union all the dataframes
"""
in this scenario the thing is the schema values of the dataframes is same but the order of the schema is different
for this we use one df schema as an array of the columns and select the columns in that particular order for all the other
dataframes
"""
print("\n\033[93m" + "Define a unified columns list and impost using selecct for all the dataframes and union all the dataframes" + "\033[0m")
# 1- schemadf, 2-json_df, 3 - parquet_df
column_headers = schemadf.columns
json_df_final = json_df.select(column_headers)
parquet_df_final = parquet_df.select(column_headers)
uniondf = schemadf.union(json_df_final).union(parquet_df_final)
uniondf.show(5)


# INFO: from uniondf Get year from txn date and rename the column as year and add one column at the end as status
# 1 for cash and 0 for credit and filter the txnno > 50000
print("\n\033[93m" + "from uniondf Get year from txn date and rename the column as year and add one column at the end as status  1 for cash and 0 for credit and filter the txnno > 50000" + "\033[0m")
year_status_df = (uniondf
				  .withColumn("txndate", expr("split(txndate, '-')[2]"))
				  .withColumnRenamed("txndate", "year")
				  .withColumn("status", expr("case when spendby='cash' then 1 else 0 end"))
				  .filter("txnno > 50000"))
year_status_df.show(5)

#INFO: find the sum of the cumulative sum for each category
print("\n\033[93m" + "find the sum of the cumulative sum for each category using aggregations" + "\033[0m")
goupby_df = year_status_df.groupby("category").agg(sum("amount").alias("total"))
goupby_df.show(5)

#INFO: casting the total as int type
print("\n\033[93m" + "casting the total as int type " + "\033[0m")
casting_df = goupby_df.withColumn("total", col("total").cast("int"))
casting_df.show(5)