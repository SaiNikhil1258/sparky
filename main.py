import os
import urllib.request
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
import sys
from collections import namedtuple
from pyspark.sql.functions import *
from pyspark.sql.types import *
import builtins
from pyspark.sql.functions import *

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['JAVA_HOME'] = r'C:\Users\LENOVO\.jdks\corretto-1.8.0_422'
# os.environ["HADOOP_HOME"] = "C:\Program Files\Hadoop"

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

import random


def motivate_me():
    li = [
        "You have been defeated today... \nWhat will you become tomorrow",
        "Being weak in anything is a disadvantage.... \nbut not a sign of INCOMPETENCE",
        "Habits become Second Nature....",
        "Being Good means Being Free",
        "The World is utterly unfair. But at the same time.. \n...Its Ruthlessly Fair",
        "Because people don't have wings... We look for ways to fly.",
        "The most meaningless thing is Do something just to DO it.",
        "No Matter How Hard or Easy the skill...\nGetting Good at something is Fun",
        "If you Really get Good.... somebody who's even better will come and find you",
        "He who would climb the ladder must begin at the bottom.",
        "We aren't limited.... to only one way of being Great!",
        "If you don’t take risks, you can’t create a future.",
        "The future belongs to those who believe in the beauty of their dreams.",
        # "If you can, cut your desires lose they'll drag you down.",
        "Iam only Here... Because My Family and Friends Brought me Here....",
    ]
    print(random.choice(li))


# print(list[random.randint(0,len(list-1))])

print()
motivate_me()
#======================================================================================================================
# note:21-9-24
# withColumn and SelectExpr

# data= """
# {
#   "country" : "US",
#   "version" : "0.6",
#   "Actors": [
#     {
#       "name": "Tom Cruise",
#       "age": 56,
#       "BornAt": "Syracuse, NY",
#       "Birthdate": "July 3, 1962",
#       "photo": "https://jsonformatter.org/img/tom-cruise.jpg",
#       "wife": null,
#       "weight": 67.5,
#       "hasChildren": true,
#       "hasGreyHair": false,
#       "picture": {
#                     "large": "https://randomuser.me/api/portraits/men/73.jpg",
#                     "medium": "https://randomuser.me/api/portraits/med/men/73.jpg",
#                     "thumbnail": "https://randomuser.me/api/portraits/thumb/men/73.jpg"
#                 }
#     },
#     {
#       "name": "Robert Downey Jr.",
#       "age": 53,
#       "BornAt": "New York City, NY",
#       "Birthdate": "April 4, 1965",
#       "photo": "https://jsonformatter.org/img/Robert-Downey-Jr.jpg",
#       "wife": "Susan Downey",
#       "weight": 77.1,
#       "hasChildren": true,
#       "hasGreyHair": false,
#       "picture": {
#                     "large": "https://randomuser.me/api/portraits/men/78.jpg",
#                     "medium": "https://randomuser.me/api/portraits/med/men/78.jpg",
#                     "thumbnail": "https://randomuser.me/api/portraits/thumb/men/78.jpg"
#                 }
#     }
#   ]
# }
# """
#
#
# df = spark.read.json(sc.parallelize([data],1))
# df.show()
# df.printSchema()
#
# from pyspark.sql.functions import *
#
# f1 = (
#
# 	df.withColumn(
# 		"Actors",
# 		expr("explode(Actors)")
#
# 	)
# )
#
# f1.show()
# f1.printSchema()
#
#
#
# f2 = f1.selectExpr(
#
# 	"Actors.Birthdate",
# 	"Actors.BornAt",
# 	"Actors.age",
# 	"Actors.hasChildren",
# 	"Actors.hasGreyHair",
# 	"Actors.name",
# 	"Actors.photo",
# 	"Actors.picture.large",
# 	"Actors.picture.medium",
# 	"Actors.picture.thumbnail",
# 	"Actors.weight",
# 	"Actors.wife",
# 	"country",
# 	"version"
# )
#
# f2.show()
#
# f2.printSchema()
# #@m
# actors = f1.select("Actors.*")
# # actors.printSchema()
# # df.selectExpr("some.Birthdate","some.BornAt","some.age","some.hasChildren","some.hasGreyHair","some.name","some.photo","some.picture","some.weight","some.wife")
#
# print(actors.columns)
#
#
# import os
# import urllib.request
# import ssl
#
# data_dir = "hadoop/bin"
# os.makedirs(data_dir, exist_ok=True)

# urls_and_paths = {
# 	"https://github.com/saiadityaus1/SparkCore1/raw/master/winutils.exe": os.path.join(data_dir, "winutils.exe"),
# 	"https://github.com/saiadityaus1/SparkCore1/raw/master/hadoop.dll": os.path.join(data_dir, "hadoop.dll"),
# }
#
# # Create an unverified SSL context
# ssl_context = ssl._create_unverified_context()
#
# for url, path in urls_and_paths.items():
# 	# Use the unverified context with urlopen
# 	with urllib.request.urlopen(url, context=ssl_context) as response, open(path, 'wb') as out_file:
# 		data = response.read()
# 		out_file.write(data)
#

# URL PYSPARK DATA





import os
import urllib.request
import ssl


urldata=(

    urllib
    .request
    .urlopen("https://randomuser.me/api/0.8/?results=10",context=ssl._create_unverified_context())
    .read()
    .decode('utf-8')

)

print(urldata)


rdd = sc.parallelize([urldata],1)

df = spark.read.json(rdd)

df.show()

df.printSchema()

from pyspark.sql.functions import *

flatten1= df.withColumn("results",expr("explode(results)"))

flatten1.show()

flatten1.printSchema()


flatten2= flatten1.selectExpr(

    "nationality",
    "results.user.cell",
    "results.user.dob",
    "results.user.email",
    "results.user.gender",
    "results.user.location.city",
    "results.user.location.state",
    "results.user.location.street",
    "results.user.location.zip",
    "results.user.md5",
    "results.user.name.first",
    "results.user.name.last",
    "results.user.name.title",
    "results.user.password",
    "results.user.phone",
    "results.user.picture.large",
    "results.user.picture.medium",
    "results.user.picture.thumbnail",
    "results.user.registered",
    "results.user.salt",
    "results.user.sha1",
    "results.user.sha256",
    "results.user.username",
    "seed",
    "version"


)



flatten2.show()

flatten2.printSchema()
#======================================================================================================================
# note:15-9-24







# df = spark.read.format("json").options(multiline=True).load("dd.json")
# data = """
# {
#     "id": 2,
#     "trainer": "Sai",
#     "zeyoaddress": {
#     	"user":{
#             "permanentAddress": "Hyderabad",
#             "temporaryAddress": "Chennai"
#     	}
#     }
# }
# """
# df = spark.read.json(sc.parallelize([data], 1))
# df.show()
# df.printSchema()
#
# fdf = df.selectExpr("id", "trainer", "zeyoaddress.user.permanentAddress", "zeyoaddress.user.temporaryAddress")
# fdf.show()
#
# fdf.printSchema()
# data= """
#
# {
#     "id": 2,
#     "trainer": "Sai",
#     "zeyoaddress": {
#         "user": {
#             "permanentAddress": "Hyderabad",
#             "temporaryAddress": "Chennai",
#             "postAddress": {
#                 "doorno": 14,
#                 "street": "sriram nagar"
#             }
#         }
#     }
# }
#
# """
# df = spark.read.json(sc.parallelize([data],1))
# df.show()
# df.printSchema()
#
# fdf = df.selectExpr("""
# 					"id",
# 					 "trainer",
# 					  "zeyoaddress.user.permanentAddress",
# 					  "zeyoaddress.user.temporaryAddress",
# 					  "zeyoaddress.user.postAddress.doorno",
# 					  "zeyoaddress.user.postAddress.street")
# """
# fdf.show()
# fdf.printSchema()
# data = """{
# 	"name":"Nikhil",
# 	"age":12,
# 	"address":{
# 		"sdf":"asdflkj",
# 		"laksjdf":"asdflkj"
# 	}
# }
# """
# rdd = sc.parallelize([data])
# df = spark.read.json(rdd)
# df.show()
# # print(df.columns)
# df.printSchema()
# ARRAY WITH STRUCT

#
#
# data= """
#
# {
#     "id": 2,
#     "trainer": "Sai",
#     "zeyoaddress": {
#         "permanentAddress": "Hyderabad",
#         "temporaryAddress": "Chennai"
#     },
#     "zeyoStudents": [
#         "Ajay",
#         "rani"
#     ]
# }
# """
# df = spark.read.json(sc.parallelize([data],1))
# df.show()
# df.printSchema()
#
# flattendf = df.selectExpr(
# 	"id",
# 	"trainer",
# 	"explode(zeyoStudents) as zeyoStudents",
# 	"zeyoaddress.permanentAddress",
# 	"zeyoaddress.temporaryAddress"
# )
#
# flattendf.show()
# flattendf.printSchema()
# Struct Inside Array


#
# data= """
#
# {
#     "id": 2,
#     "trainer": "Sai",
#     "zeyoStudents": [
#         {
#             "user": {
#                 "name": "Ajay",
#                 "age": 21
#             }
#         },
#         {
#             "user": {
#                 "name": "Rani",
#                 "age": 24
#             }
#         }
#     ]
# }
#
# """
# df = spark.read.json(sc.parallelize([data],1))
# df.show()
# df.printSchema()
#
#
#
# flatten1 = df.selectExpr(
#
# 	"id",
# 	"trainer",
# 	"explode(zeyoStudents) as zeyoStudents"
# )
#
# flatten1.show()
#
# flatten1.printSchema()
#
#
#
#
# flatten2 = flatten1.selectExpr(
#
# 	"id",
# 	"trainer",
# 	"zeyoStudents.user.age",
# 	"zeyoStudents.user.name"
#
#
#
# )
#
# flatten2.show()
#
# flatten2.printSchema()


#======================================================================================================================
# 14-9-24
#note: DOUBT the hadoop home is set in the os level still we are needed to specify
# the env variable seperately in the intellij is it this IDE perk or what could
# be the reason

# rdd1 = spark.sparkContext.parallelize([
# 	("sai", 10),
# 	("zeyo", 20),
# 	("sai", 15),
# 	("zeyo", 10 ),
# 	("sai", 10 ),
# ],1)
#
# df = rdd1.toDF(['name', 'amt']).coalesce(1)
# df.write.format("json").mode("overwrite").save("something")
# # Write Code Intellij/Pycharm
#
# import os
# import urllib.request
# import ssl
#
# data_dir = "hadoop/bin"
# os.makedirs(data_dir, exist_ok=True)
#
# urls_and_paths = {
# 	"https://github.com/saiadityaus1/SparkCore1/raw/master/winutils.exe": os.path.join(data_dir, "winutils.exe"),
# 	"https://github.com/saiadityaus1/SparkCore1/raw/master/hadoop.dll": os.path.join(data_dir, "hadoop.dll"),
# }
#
# # Create an unverified SSL context
# ssl_context = ssl._create_unverified_context()
#
# for url, path in urls_and_paths.items():
# 	# Use the unverified context with urlopen
# 	with urllib.request.urlopen(url, context=ssl_context) as response, open(path, 'wb') as out_file:
# 		data = response.read()
# 		out_file.write(data)
#
#
# # os.environ['HADOOP_HOME'] = "hadoop"
#
#
# data = [
# 	("00000000", "06-26-2011", 200, "Exercise", "GymnasticsPro", "cash"),
# 	("00000001", "05-26-2011", 300, "Exercise", "Weightlifting", "credit"),
# 	("00000002", "06-01-2011", 100, "Exercise", "GymnasticsPro", "cash"),
# 	("00000003", "06-05-2011", 100, "Gymnastics", "Rings", "credit"),
# 	("00000004", "12-17-2011", 300, "Team Sports", "Field", "paytm"),
# 	("00000005", "02-14-2011", 200, "Gymnastics", None, "cash")
# ]
#
# rdd = spark.sparkContext.parallelize(data,1)
# columns = ["txnno", "txndate", "amount", "category", "product", "spendby"]
# csvdf = rdd.toDF(columns)
#
# csvdf.write.format("json").mode("overwrite").save("d")
#======================================================================================================================
# 08-9-24
# rdd1 = spark.sparkContext.parallelize([
# 	("sai", 10),
# 	("zeyo", 20),
# 	("sai", 15),
# 	("zeyo", 10 ),
# 	("sai", 10 ),
# ],1)
#
# df = rdd1.toDF(['name', 'amt']).coalesce(1)
# dept = Window.partitionBy("name")
#
# # Apply a window function, e.g., row_number
# df_with_rank = df.withColumn("row_number", row_number().over(dept.orderBy("amt")))
#
# # Show the result
# df_with_rank.show()
# df.show()
# aggdf = df.groupby("name").agg(
# 	sum("amt").alias("total"),
# 	count("name").alias("cnt"),
# 	collect_list("amt").alias("col_collect_list"),
# 	collect_set("amt").alias("col_collect_set")
# )
# aggdf.show()

# data = [("DEPT3", 500),
# 		("DEPT3", 200),
# 		("DEPT1", 1000),
# 		("DEPT1", 1000),
# 		("DEPT1", 500),
# 		("DEPT2", 400),
# 		("DEPT2", 200)]
# columns = ["dept", "salary"]
#
# # Create DataFrame
# df = spark.createDataFrame(data, columns)
# df.show()
#
# ## Defined My Window
#
# deptwindow = Window.partitionBy("dept").orderBy(col("salary").desc())
#
# ## Applying window with Dense Rank
#
# dfrank = df.withColumn("drank", dense_rank().over(deptwindow))
#
# dfrank.show()
#
# filterdf= dfrank.filter("drank=2")
#
# filterdf.show()
#
# finaldf = filterdf.drop("drank")
#
# finaldf.show()


# print()
# print("======= FILE 1 =======")
# print()
#
# file1 = sc.textFile("file1.txt",1)
#
# gymdata = file1.filter(lambda x :  'Gymnastics' in x)
#
# mapsplit = gymdata.map(lambda x : x.split(","))
#
# from collections import namedtuple
#
# schema = namedtuple("schema",["txnno","txndate","custno","amount","category","product","city","state","spendby"])
#
# schemardd = mapsplit.map(lambda x : schema(x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8]))
#
# prodfilter = schemardd.filter(lambda x : 'Gymnastics' in x.product)
#
# schemadf = prodfilter.toDF()
#
# schemadf.show(5)
#
# print()
# print("=======csv df =======")
# print()
#
#
#
# csvdf = spark.read.format("csv").option("header","true").load("file3.txt")
#
# csvdf.show(5)
#
#
# print()
# print("=======JSON df =======")
# print()
#
# jsondf = spark.read.format("json").load("file4.json")
#
# jsondf.show(5)
#
#
# print()
# print("=======parquet df =======")
# print()
#
#
# parquetdf = spark.read.load("file5.parquet")
#
# parquetdf.show(5)
#
# collist= ["txnno", "txndate", "custno", "amount", "category", "product", "city", "state", "spendby"]
#
#
# schemadf1= schemadf.select(*collist)
# csvdf1 = csvdf.select(*collist)
# jsondf1 = jsondf.select(*collist)
# parquetdf1=parquetdf.select(*collist)
#
# schemadf1.show(5)
# csvdf1.show(5)
# jsondf1.show(5)
# parquetdf1.show(5)
#
#
#
#
# uniondf = schemadf1.union(csvdf1).union(jsondf1).union(parquetdf1)
#
# print()
# print("=======uniondf =======")
# print()
#
# uniondf.show(5)
#
# from pyspark.sql.functions import  *
#
# procdf = ( uniondf.withColumn("txndate",expr("split(txndate,'-')[2]"))
# 		   .withColumnRenamed("txndate","year")
# 		   .withColumn("status",expr("case when spendby='cash' then 1 else 0 end"))
# 		   .filter("txnno > 50000")
# 		   )
#
#
# procdf.show(10)
#
# print()
# print("=======agg df =======")
# print()
#
# from pyspark.sql.types import *
#
# aggdf = procdf.groupby("category").agg(sum("amount").alias("total"))
# agdf= aggdf.withColumn("total", col("total").cast("int"))
# aggdf.show()
# agdf.show()
#======================================================================================================================
# 07-9-24
# df = spark.read.format('csv').option("header","true").load("usdata.csv")
# # df.show()
# filter_df = df.filter("state='LA'")
# filter_df.show()
# time = datetime.now()
# date_df = filter_df.withColumn("tdate_with_time", lit(time)).select("first_name", "last_name", "company_name", "address", "email", "tdate_with_time")
# date_df.show(truncate=False)

# 	.withColumn("amount", expr("amount+100"))
# rdd1 = spark.sparkContext.parallelize([
# 	(1, 'raj'),
# 	(2, 'ravi'),
# 	(3, 'sai'),
# 	(5, 'rani')
# ],1)
#
# rdd2 = spark.sparkContext.parallelize([
# 	(1, 'mouse'),
# 	(3, 'mobile'),
# 	(7, 'laptop')
# ],1)
#
# df1 = rdd1.toDF(['id', 'name']).coalesce(1)
# df2 = rdd2.toDF(['id', 'product']).coalesce(1)
# # Show the DataFrames
# df1.show()
# df2.show()
# listvalue= df2.select("id").rdd.flatMap(lambda x : x).collect()
# print(listvalue)
# filterdf = df1.filter( ~ df1['id'].isin(listvalue))
# print()
# print("====LIST FILTERING===")
# print()
# filterdf.show()
#
# ##  ANTI JOINNNNNNNNNNNNNNNNNNNNNNNNNNNNN
# print()
# print("====ANTI JOIN===")
# print()
# antijoin=df1.join(df2,  ["id"]  ,"left_anti")
# antijoin.show()
# print()
# print("====Cross Join==")
# print()
# crossj=df1.crossJoin(df2)
# crossj.show()

#======================================================================================================================
# 01-9-24
#
# data = [
# 	("00000000", "06-26-2011", 200, "Exercise", "GymnasticsPro", "cash"),
# 	("00000001", "05-26-2011", 300, "Exercise", "Weightlifting", "credit"),
# 	("00000002", "06-01-2011", 100, "Exercise", "GymnasticsPro", "cash"),
# 	("00000003", "06-05-2011", 100, "Gymnastics", "Rings", "credit"),
# 	("00000004", "12-17-2011", 300, "Team Sports", "Field", "paytm"),
# 	("00000005", "02-14-2011", 200, "Gymnastics", "", "cash")
# ]
#
# rdd = spark.sparkContext.parallelize(data)
# columns = ["txnno", "txndate", "amount", "category", "product", "spendby"]
# csvdf = rdd.toDF(columns)
# print()
# print("===== raw dataframe ===")
# print()
#
# csvdf.show()
# procdf = (
# 	csvdf.withColumn("category", expr("upper(category)"))
# 	.withColumn("amount", expr("amount+100"))
# 	.withColumn("txnno", expr("cast(txnno as int)"))
# 	.withColumn("txndate", expr("split(txndate, '-')"))
# 	.withColumn("product", expr("lower(product)"))
# 	.withColumn("spendby", expr("spendby"))
# 	.withColumn("status", expr("case when spendby='cash' then 0 else 1 end"))
# 	.withColumn("con", expr("concat(category,'~SAI')"))
# )
# procdf.show()
# procdf = (
#
# csvdf.withColumn("txndate", expr("split(txndate,'-')[2]"))
# 	.withColumnRenamed("txndate", "year")
#
# )
#
# procdf.show()


# Scenario

# source_rdd = spark.sparkContext.parallelize([
# 	(1, "A"),
# 	(2, "B"),
# 	(3, "C"),
# 	(4, "D"),
# ], 1)
#
# target_rdd = spark.sparkContext.parallelize([
# 	(1, "A"),
# 	(2, "B"),
# 	(4, "X"),
# 	(5, "F")
# ], 1)
# print(source_rdd.collect())
# print()
# print(target_rdd.collect())

# Convert RDDs to DataFrames using toDF()
#  df1 = source_rdd.toDF(["id", "name"])
# df2 = target_rdd.toDF(["id", "name1"])
#
# # Show the DataFrames
# df1.show()
# df2.show()
#
# print()
# print("====FULL JOIN====")
# print()
#
# fulljoin = df1.join(df2, ["id"], "full")
# fulljoin.show()
#
# print()
# print("===Condition====")
# print()
#
# condition = (
# 	fulljoin.withColumn(
# 		"comment",
# 		expr("case when name=name1 then 'match' else 'mismatch' end"))
# )
#
# condition.show()
#
# print()
# print("===Filter Condition====")
# print()
#
# filter = condition.filter("comment !='match'")
# filter.show()
#
# print()
# print("===null Condition====")
# print()
#
# nullcondition = (
# 	filter.withColumn(
# 		"comment",
# 		expr("case when name1 is null then 'New in Source' when name is null then 'new in Target' else comment end "))
# )
#
# nullcondition.show()
#
# print()
# print("===Final df====")
# print()
#
# finaldf = nullcondition.drop("name", "name1")
# finaldf.show()

#======================================================================================================================
#31-8-24
#
# print()
# print("===== Category not equal to Exercise ===")
# print()
#
# notfilter = csvdf.filter(" category != 'Exercise' ")
# notfilter.show()
#
# print()
# print("===== category=Exercise and spendby=cash ===")
# print()
#
# notfil1 = csvdf.filter(" category='Exercise' and spendby !='cash' ")
# notfil1.show()
# df = spark.read.format("csv").load("gym_equipment_sales.csv", header=True)
# # df.show()
# df.describe().show()


# data = [
# 	("00000000", "06-26-2011", 200, "Exercise", "GymnasticsPro", "cash"),
# 	("00000001", "05-26-2011", 300, "Exercise", "Weightlifting", "credit"),
# 	("00000002", "06-01-2011", 100, "Exercise", "GymnasticsPro", "cash"),
# 	("00000003", "06-05-2011", 100, "Gymnastics", "Rings", "credit"),
# 	("00000004", "12-17-2011", 300, "Team Sports", "Field", "paytm"),
# 	("00000005", "02-14-2011", 200, "Gymnastics", None, "cash")
# ]
#
# rdd = spark.sparkContext.parallelize(data)
# columns = ["txnno", "txndate", "amount", "category", "product", "spendby"]
# csvdf = rdd.toDF(columns)
# print()
# print("===== raw dataframe ===")
# print()
#
# csvdf.show()
#
# print()
# print("===== Proc Dataframe ===")
# print()

# procdf = (
# 	csvdf.selectExpr(
#
# 		"txnno",   # column select
# 		"txndate",  # column select
# 		"amount*2  as amount",   # Expression
# 		"upper(category)      as category",   # Expression
# 		"lower(product) as product",   # Expression
# 		"spendby"   #  column select
# 	)
# )
# procdf.show()
# procdf=(
#
# 	csvdf.selectExpr(
# 		"cast(txnno as int) as txnno",
# 		"split(txndate,'-')[2] as year",
# 		"amount+100   as   amount",
# 		"upper(category) as category",
# 		"lower(product) as product",
# 		"spendby",
# 		"""case
# 			when spendby='cash' then 0
# 			when spendby='paytm' then 2
# 			else 1
# 			end
# 			as status""",
# 	)
# )

# procdf=(
# 	csvdf.selectExpr(
# 		"txnno",
# 		"txndate",
# 		"amount",
# 		"category",
# 		"product",
# 		"spendby",
# 		"concat(category,'~SAI') as con"
# 	)
# )
# procdf.show()
#======================================================================================================================
#25-8-24
# df1 = spark.read.format("csv").load("prod.csv")
# df1.createOrReplaceTempView("one_df")
# one_df = spark.sql("select * from one_df")
# one_df.show()
#
# df2 = spark.read.format("json").load("file4.json")
# df2.createOrReplaceTempView("two_df")
# two_df =spark.sql("select * from two_df")
# two_df.show()
# df3 = spark.read.load("file5.parquet")
# df2.createOrReplaceTempView("three_df")
# three_df = spark.sql("select * from three_df")
# three_df.show()

#NOTE: Dataframe
# df = spark.read.format("csv").load("dt.txt").toDF("txNo","txndate","amount","category","product","spendby")
# df.show()
# df1 = df.select("txNo","amount","product")
# df1.show()
# df2 = df.drop("txNo","amount")
# df2.show()
# one_df = df.filter("category = 'Exercise'")
# print("one filter")
# one_df.show()
# multi_df = df.filter("category = 'Exercise' and spendby='cash'")
# print("multi and filter")
# multi_df.show()
# or_df = df.filter("category = 'Exercise' or spendby='cash'")
# print("multi filter with or")
# or_df.show()
# x1 = df.filter("category in ('Exercise','Gymnastics')")
# print("multi filter in the same column")
# x1.show()
# gym_df = df.filter("product like '%Gymnastics%'")
# print("like filter")
# gym_df.show()
# null_df = df.filter("product is null")
# print("null filter")
# null_df.show()
# not_null_df = df.filter("product is not null")
# print("not Null filter")
# not_null_df.show()

# res = df.filter("category = 'Exercise'").drop("product").select("txndate","amount","category","spendby")
# print("using multiple methods")
# res.show()
#

#======================================================================================================================
#NOTE: 24-8-24

# from collections import namedtuple
# data = sc.textFile("dt.txt")
# data.foreach(print)
# print()
# split_data = data.map(lambda x:x.split(","))
# schema = namedtuple("scheme", ["TxnNo","TxnDate","amount","category","product","spendby"])
# schema_rdd = split_data.map(lambda x:schema(x[0],x[1],x[2],x[3],x[4],x[5]))
# filter_data = schema_rdd.filter(lambda x:"Gymnastics" in x.product)
# filter_data.foreach(print)
# df = filter_data.toDF()
# df.show()

# df = spark.read.format("csv").option("header", "true").load("prod.csv").show()
# x = df[df["id">"4"]]
# x.show()

#TODO: doubt of i have done this  with the index and filter and when i use .show() method on the converted df
# iam getting an error.... help me here (later the session)

# data = sc.textFile("dt.txt")
# # data.foreach(print)
# GymData = data.filter(lambda x: "Gymnastics" in x.split(",")[4])
# GymData.foreach(print)

#INFO: do we need to provide the schema to convert to DF

#
# df = GymData.toDF()
# df.show


# filter_data = data.filter(lambda x: "Gymnastics" in x)
# print("Filtered Gymnastics Data from dt.txt file")
# filter_data.foreach(lambda x: print(x))
#
# # Further filter based on the 5th column
#
# # Convert RDD to DataFrame
# schema = ["Column1", "Column2", "Column3", "Column4", "Column5", "Column6"]
# GymDataDF = GymData.map(lambda x: x.split(",")).toDF(schema)
#
# # Show the DataFrame
# print("Pure Gymnastics Data")
# GymDataDF.show()
#
# filter_data = data.filter(lambda x: "Gymnastics" in x)
# print()
# print("Filtered Gymnastics Data from dt.txt file")
# filter_data.foreach(print)
# GymData = data.filter(lambda x: "Gymnastics" in x.split(",")[4])
# print()
# # schema_rdd = mapsplit.map(lambda x:schema(x[0],x[1],x[2],x[3],x[4],x[5]))
# print("Pure Gymnastics Data")
# GymData.foreach(print)
# schema = ["Column1", "Column2", "Column3", "Column4", "Column5", "Column6"]
# GymDataDF = GymData.map(lambda x: x.split(",")).toDF(schema)
#
# # Show the DataFrame
# print("Pure Gymnastics Data")
# GymDataDF.show()
#
# res.foreach(print)
#======================================================================================================================
# 18-8-24
# x = ["State->TN~City->Chennai", "State->Kerala~City->Trivandrum"]
# print("Raw string list")
# print(x)
#
# string_rdd = sc.parallelize(x)
# print("RAW RDD")
# print(string_rdd.collect())
#
# print()
# flat_rdd = string_rdd.flatMap(lambda x : x.split("~"))
# print(flat_rdd.collect())
# print()
# print("separating according to the State and City into RDD")
# state_rdd = flat_rdd.filter(lambda x : "State" in x)
# print()
# print(state_rdd.collect())
# City_rdd = flat_rdd.filter(lambda x : "City" in x)
# print(City_rdd.collect())
#
# print()
# state = state_rdd.map(lambda x : x.replace("State->", ""))
# city = City_rdd.map(lambda x : x.replace("City->", ""))
# print("Separated Lists of city and Sate")
# print()
# print(state.collect())
# print(city.collect())
#

# print("File Reading")
# data = sc.textFile("state.txt")
# print(data.collect())
#
# flat_rdd = data.flatMap(lambda x : x.split(","))
# print(flat_rdd.collect())
# print()
# print("separating according to the State and City into RDD")
# state_rdd = flat_rdd.filter(lambda x : "State" in x)
# City_rdd = flat_rdd.filter(lambda x : "City" in x)
# state_rdd.foreach(print)
# City_rdd.foreach(print)
# print()
# state = state_rdd.map(lambda x : x.replace("State->", ""))
# city = City_rdd.map(lambda x : x.replace("City->", ""))
# print("Separated Lists of city and Sate")
# state.foreach(print)
# city.foreach(print)
#

# data = sc.textFile("usdata.csv")
# x = data.filter(lambda x : len(x) > 200)
# y = x.flatMap(lambda x : x.split(","))
# # y.foreach(print)
# z = y.map(lambda x : x.replace("-", ""))
# a = z.map(lambda x : x + ", Zeyo")
# a.foreach(print)
#
#
# data = sc.textFile("dt.txt")
# data.foreach(print)
# filter_data = data.filter(lambda x : "Gymnastics" in x)
# print()
# print("Filtered Gymnastics Data from dt.txt file")
# filter_data.foreach(print)
#===================================================================================================================================================
# 17-8-24
# string_list = ["Sai", "Sqoop", "Spark", "Sqoop Apache"]
# print("STRING RDD")
# string_rdd = sc.parallelize(string_list)
# print(string_rdd.collect())
# add_pvt = string_rdd.map(lambda x : x + " pvt")
# print(add_pvt.collect())
#
# replace_rdd = string_rdd.map(lambda x : x.replace("Sqoop", "Hadoop"))
# print(replace_rdd.collect())
#
# filter_rdd = string_rdd.filter(lambda x : "Sqoop" in x)
# print(filter_rdd.collect())
#


# x = ["A~B", "C~D"]
# h =[]
# # for i in x:
# # 	for y in i:
# # 		# y.split("~")
# # 		h.append(y.split("~"))
# # print(h)
# # x = [item for element in x for item in element.split("~")]
# # print(x)
# # a,b = x.split("~")
#
# for i in x:
# 	parts = i.split("~")
# 	for part in parts:
# 		h.append(part)
# print(h)
# h = "".join(h)
# print(h)

# x = ["A~B", "C~D"]
# print(x)
# x_rdd = sc.parallelize(x)
# print("Rdd Data before splitting")
# print(x_rdd.collect())
# flat = x_rdd.flatMap(lambda x : x.split("~"))
# print("Rdd Data after splitting")
# print(flat.collect())


# x = ["State->TN~City->Chennai", "State->Kerala~City->Trivandrum"]
# print(x)
# states = []
# cities = []
# res = []
# for i in x:
# 	# print(i.split("~"))
# 	parts = i.split("~")
# 	res.extend(parts)
#
# for i in res:
# 	if i.split("->")[0] == "State":
# 		states.append(i.split("->")[1])
# 	else:
# 		cities.append(i.split("->")[1])

# print(res)
# print(parts)
# state = parts[0].split("->")[1]
# city = parts[1].split("->")[1]
# states.append(state)
# cities.append(city)
#
# # for part in parts:
# # 	# print(part.split("->")[1])
# # 	# p1 = part.split("->")
# # 	# if p1[0] == 'State':
# # 	# 	state.append(p1[1])
# # 	# else:
# # 	# 	city.append(p1[1])
# # 	print(part)
#
# 	# print(part.split("->")[0])
# print(states)
# print(cities)


# raw_str = ["ApacheSpark", "ApacheSqoop", "Hadoop"]
# print("raw String array")
# print(raw_str)
# rdd_str = sc.parallelize(raw_str)
# print("Converted to RDD")
# print(rdd_str.collect())
# replaced_rdd = rdd_str.map(lambda x : x.replace("Apache", ""))
# print("After replacing the Apache")
# print(replaced_rdd.collect())


# Free Datasets Download
# 17-8-24
# Free Datasets Download
# import os, urllib.request, ssl
# ssl_context = ssl._create_unverified_context()
# [open(path, 'wb').write(urllib.request.urlopen(url, context=ssl_context).read()) for url, path in {
# 	"https://github.com/saiadityaus1/test1/raw/main/df.csv": "df.csv",
# 	"https://github.com/saiadityaus1/test1/raw/main/df1.csv": "df1.csv",
# 	"https://github.com/saiadityaus1/test1/raw/main/dt.txt": "dt.txt",
# 	"https://github.com/saiadityaus1/test1/raw/main/file1.txt": "file1.txt",
# 	"https://github.com/saiadityaus1/test1/raw/main/file2.txt": "file2.txt",
# 	"https://github.com/saiadityaus1/test1/raw/main/file3.txt": "file3.txt",
# 	"https://github.com/saiadityaus1/test1/raw/main/file4.json": "file4.json",
# 	"https://github.com/saiadityaus1/test1/raw/main/file5.parquet": "file5.parquet",
# 	"https://github.com/saiadityaus1/test1/raw/main/file6": "file6",
# 	"https://github.com/saiadityaus1/test1/raw/main/prod.csv": "prod.csv",
# 	"https://github.com/saiadityaus1/test1/raw/main/state.txt": "state.txt",
# 	"https://github.com/saiadityaus1/test1/raw/main/usdata.csv": "usdata.csv"
# }.items()]


# NOTE: 10-8-24
# # spark.read.format("csv").load("data/test.txt").toDF("Success").show(20, False)
# # a = 2
# # print(a)
# # b = 4
# # print(b)
# #
# #
# # def hello(name):
# # 	print(f"hello {name}")
# #
# # hello("Nikhil")
# print("\n\n")
# # print("=====It Started=====")
# # a = 2
# # print(a)
# # b = a + 1
# # print(b)
# # c = "Nikhil"
# # print(c)
# # l = [1,2,3]
# # # print(l)
# # # l = [x**2 for x in l]
# # print(l)
# # print("=======RDDDDDDDDDDDDDDDD===========")
# # rdd = sc.parallelize(l)
# # print(rdd.collect())
# # proclist = rdd.map(lambda x: x + 10)
# # print(proclist.collect())
# d = [1,2,3]
# print("original list")
# print(d)
#
#
# rdd = sc.parallelize(d)
# print("conversion of RDD")
# print(rdd.collect())
# #
# #
# # c = rdd.map(lambda x : x+2)
# # print(c.collect())
# print("ADDING 10 TO RDD")
# a = rdd.map(lambda x : x + 10)
# print(a.collect())
#
# print("MULTIPLIED LIST")
# mullist = rdd.map(lambda x : x * 10)
# print(mullist.collect())
#
#
# print("FILTERED LIST")
# filtered_list = rdd.filter(lambda x : x > 2)
# print(filtered_list.collect())
#
#
