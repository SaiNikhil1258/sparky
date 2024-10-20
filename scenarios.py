import os
import urllib.request
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession, Window
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

# SPARK  BOILERPLATE
# NOTE: spark scenario number 1

# data = [
# 	("A", "AA"),
# 	("B", "BB"),
# 	("C", "CC"),
# 	("AA", "AAA"),
# 	("BB", "BBB"),
# 	("CC", "CCC")
# ]
#
# df = spark.createDataFrame(data, ["child", "parent"],1)
# df.show()
#
# print("WITH SPARK DSL")
# df_aliased = df.alias("df1").join(
# 	df.alias("df2"),
# 	col("df1.parent") == col("df2.child"),
# 	"inner"
# ).select(
# 	col("df1.child").alias("child"),
# 	col("df1.parent").alias("parent"),
# 	col("df2.parent").alias("GrandParent"),
# )
#
# # Show the result
# df_aliased.show()
# print("WITH SPARK SQL")
# df1 = df.alias("df1")
# df2 = df.alias("df2")
# df1.createOrReplaceTempView("df1")
# df2.createOrReplaceTempView("df2")
#
# spark.sql("""
# 	select a.child, a.parent, b.parent
# 	from df1 a  join df2 b
# 	on a.parent=b.child
# """).show()
#
#
# print("using concat and hardcoded not applicable for other type of scenarios")
# res = df.withColumn('GrandParent', concat(df.parent,df.child))
# res.limit(3).show()
#

# NOTE: spark scenario-2
# data = [(1,), (2,), (3,), (1,), (2,), (1,)]
#
# df = spark.createDataFrame(data, ["id"],1)
#
#
# df.show()
#
# df1=df  # FIRST DF
# df2=df  # Second df
# df1.createOrReplaceTempView("df1")
# df2.createOrReplaceTempView("df2")
# # NOTE: SPARK SQL INNER Join
# print("NOTE: SPARK SQL INNER &  OUTER Join")
# spark.sql("""
# 		select * from df1 a inner join df2 b on a.id=b.id
# """).show()
# # NOTE: SPARK SQL OUTER Join
# spark.sql("""
# 		select * from df1 a full outer join df2 b on a.id=b.id
# """).show()
#
#
# # NOTE: SPARK DSL INNER &  OUTER Join
# print("NOTE: SPARK DSL INNER &  OUTER Join")
# inner_join_df = df.alias("df1").join(df.alias("df2"), on="id",how="inner")
# outer_join_df = df.alias("df1").join(df.alias("df2"), on="id",how="outer")
# inner_join_df.show()
# outer_join_df.show()


#
# # NOTE: spark scenario-3
# data = [
# 	('A', 'D', 'D'),
# 	('B', 'A', 'A'),
# 	('A', 'D', 'A')
# ]
# # goupby_df = year_status_df.groupby("category").agg(sum("amount").alias("total"))
# df = spark.createDataFrame(data).toDF("TeamA", "TeamB", "Won")
# df.show()
#
# # NOTE: if we do normal group by and order by we would not be able to get the data of the teams that have not won
# #  single game to get around this we need to get all teams participating
# all_teams = df.select(col("TeamA").alias("team")).union(df.select(col("TeamB").alias("team"))).distinct()
# print("\nall teams")
# all_teams.show()
#
# # NOTE:  we then get the teams grouped by the wins that they have and make it into another df
# grouped_df = df.groupBy("Won").agg(count("Won").alias("wins_count"))
# print("\ngrouped df")
# grouped_df.show()
#
# # note: after this we just join both the df's (all_teams and grouped_by) for left on all_teams.team == grouped_df.won
# #  since we need all the teams
# #  get it.
# final_df = all_teams.join(grouped_df, all_teams.team == grouped_df.Won, "left").select("team", "wins_count")
# # we just neeed to fill the null values with 0 there are other methods to do which are better this is just a method just
# # so you know
# res_df = final_df.withColumn("wins_count", expr("CASE WHEN wins_count IS NULL THEN 0 ELSE wins_count end")).orderBy(
# 	"team")
# res_df.show()
# #
# # # INFO: if you only want the data of the teams that have won
# # res_df = df.groupby("Won").agg(count("won").alias("wins_count")).orderBy(col("wins_count").desc())
# # res_df.show()
#
# # note: spark sql
#
# print("\n\033[93m" + "Using Spark SQL" + "\033[0m")
# df.createOrReplaceTempView("games")
#
# # SQL query to get distinct teams from both TeamA and TeamB
# query = """SELECT TeamA AS team FROM games
# UNION
# SELECT TeamB AS team FROM games
# """
# # SELECT DISTINCT team
# # ) AS all_teams
#
# # Execute SQL query
# all_teams_df = spark.sql(query)
# # Show result
# print("\n\033[93m" + "Using Spark SQL all_teams participated" + "\033[0m")
# all_teams_df.show()
#
# team_wins = spark.sql("""
# 	select Won, count(won) as wins_count from games group by won
# """)
# print("\n\033[93m" + "Using Spark SQL teams wins" + "\033[0m")
# team_wins.show()
# all_teams_df.createOrReplaceTempView("all_teams")
# team_wins.createOrReplaceTempView("team_wins")
#
# res = spark.sql("""
# 	select team,
# 	case when wins_count is NULL then 0 else wins_count end as wins_count
# 	from all_teams left join team_wins on team=Won
# 	order by team
# """)
# print("\n\033[93m" + "Using Spark SQL  resultant df" + "\033[0m")
# res.show()






# # NOTE: spark scenario-4
# #note: given the two dfs we would want the resultant data frame as
# """
# +---+-----+------+
# | id| name|salary|
# +---+-----+------+
# |  1|Henry|   100|
# |  2|Smith|   500|
# |  3| Hall|     0|
# +---+-----+------+
# """
# data1 = [
# 	(1, "Henry"),
# 	(2, "Smith"),
# 	(3, "Hall")
# ]
# columns1 = ["id", "name"]
# rdd1 = sc.parallelize(data1, 1)
# df1 = rdd1.toDF(columns1)
# df1.show()
# data2 = [
# 	(1, 100),
# 	(2, 500),
# 	(4, 1000)
# ]
# columns2 = ["id", "salary"]
# rdd2 = sc.parallelize(data2, 1)
# df2 = rdd2.toDF(columns2)
# df2.show()
# # info: this is the classic left join and fill na values in the df
# res = (df1.join(df2, "id", "left")
# 	   .withColumn("salary", expr("case when salary is NULL then 0 else salary end"))
# 	   .orderBy("id"))
# print("\n\033[93m" + "the left joined resultant df" + "\033[0m")
# res.show()
#
# sql_df = df1.createOrReplaceTempView("sql_df")
# sql_dff = df2.createOrReplaceTempView("sql_dff")
#
# # select a.id, a.name,
# # 			case when b.salary is null then 0 else b.salary end as salary
# sql = spark.sql("""
# select a.id, a.name, coalesce(b.salary,'nikhil') as salary
# 			from sql_df a
# 			left join sql_dff b
# 			on a.id == b.id
# 			order by a.id
# """)
# print("\n\033[93m" + "the spark sql result" + "\033[0m")
# sql.show()




# NOTE: spark scenario-5
# data = [
# 	(101, "Eng", 90),
# 	(101, "Sci", 80),
# 	(101, "Mat", 95),
# 	(102, "Eng", 75),
# 	(102, "Sci", 85),
# 	(102, "Mat", 90)
# ]
# columns = ["Id", "Subject", "Marks"]
# rdd = spark.sparkContext.parallelize(data)
# df = rdd.toDF(columns)
# print("\n\033[93m" + "The raw DF" + "\033[0m")
# df.show()
# df.createOrReplaceTempView("marks")
# # you can also pass the agg function as a dictionary as below
# # pivoted_df = df.groupby("Id").pivot("Subject").agg({"Marks":"max"})
# pivoted_df = df.groupby("Id").pivot("Subject").agg(first("Marks"))
# print("\n\033[93m" + "The Pivoted DF with Spark DSL" + "\033[0m")
# pivoted_df.show()
# # info: the pivot will transpose the selected column values into new columns
# res = spark.sql("""
# select * from
# (
# 	select Id, Subject, Marks
# 	from marks
# )
# pivot (
# 	first(Marks) for subject in ('Eng', 'Sci', 'Mat')
# )
# order by Id
# """)
# print("\n\033[93m" + "The Pivoted DF with Spark SQL" + "\033[0m")
# res.show()
#
#
# pivoted_df_sql = spark.sql("""
#     SELECT
#         Id,
#         MAX(CASE WHEN Subject = 'Eng' THEN Marks END) AS Eng,
#         MAX(CASE WHEN Subject = 'Sci' THEN Marks END) AS Sci,
#         MAX(CASE WHEN Subject = 'Mat' THEN Marks END) AS Mat
#     FROM marks
#     GROUP BY Id
#     ORDER BY Id
# """)
# pivoted_df_sql.show()






# note scenario-6
#  return the store_id, and the number of the entries where the number of entries is max
# Q1 :
# Input:
# strore,entries
# 1,p1,p2,p3,p4
# 2,p1
# 3,p1,p2
# 4,p1,p2,p3,p4,p5,p6,p7
#
# Output:
# 4,7

# cols = ["store", "entries"]
# data = [
# 	( "1",["p1","p2","p3","p4" ]),
# 	( "2",["p1"] ),
# 	( "3",["p1","p2" ]),
# 	( "4",["p1","p2","p3","p4","p5","p6","p7" ])
# ]
#
# rdd = sc.parallelize(data)
# df = rdd.toDF(cols)
# df.show()
# df.printSchema()
#
# df = df.withColumn("length",size(col("entries")))
# df.show()
# max_length = df.agg(max("length")).collect()[0][0]
# print(max_length)
# df.filter(col("length") == max_length).show()







# note -7
#  finally need total the sub if any one know share the code using spark scala
# Q2:
# Input
# RollNo, name,tamil,eng,math,sci,social
# 203040, Rajesh, 10, 20, 30, 40, 50
#
# Output:
# RollNo,name,Tamil,eng,math,sci,social ,tot
# 203040, Rajesh, 10, 20, 30, 40, 50,150

# cols = ["RollNo", "name","tamil","eng","math","sci","social"]
# data = [
# 	( "203040", "Rajesh", "10", "20", "30", "40", "50" ),
# 	( "203046", "Rsdflkjajesh", "13", "31", "56", "64", "64" ),
# 	( "203058", "Rajessdfh", "64", "40", "48", "62", "63" ),
# ]
#
# rdd = sc.parallelize(data)
# df = rdd.toDF(cols)
# df.show()
# df.withColumn("total", df.tamil+df.eng+df.math+df.sci+df.social).show()
#






# note -8
# Input :
# Mail,mob
# Renuka1992@gmail.com,9856765434
# anbu.arasu3@gmail.com,9844567788
#
# Output:
# Mail,mob
# R********2@gmail.com,98****34
# a*********3@gmail.com,98****88

# cols = ["Mail","mob"]
# data = [
# 	( "Renuka1992@gmail.com","9856765434" ),
# ( "anbu.arasu3@gmail.com","9844567788" ),
# ( "sainihil123123@gmail.com","9844567788" ),
# ]
# rdd = sc.parallelize(data)
# df = rdd.toDF(cols)
# df.show()
# df.createOrReplaceTempView("df")
#
# masked_df = spark.sql("""
# SELECT
#     CONCAT(SUBSTR(Mail, 1, 1), '*****', SUBSTR(Mail, LENGTH(SUBSTRING_INDEX(Mail, '@', +1)) + 1)) AS masked_email,
#     CONCAT(SUBSTR(mob, 1, 2), '****', SUBSTR(mob, -2)) AS masked_phone
# FROM df
# """)
#
# # Show the result
# masked_df.show(truncate=False)
#
#
# # info using the UDF
# def mask_email(email):
# 	if "@" in email:
# 		local_part, domain_part = email.split("@")
# 		masked_email = local_part[0] + "*****" + local_part[-1] + "@" + domain_part
# 		return masked_email
# 	return email  # Return as is if no '@' found
#
# # Define UDF to mask phone number
# def mask_phone(phone):
# 	if len(phone) > 4:
# 		masked_phone = phone[:2] + "****" + phone[-2:]
# 		return masked_phone
# 	return phone  # Return as is if phone number is too short
#
# # Register UDFs
# mask_email_udf = udf(mask_email, StringType())
# mask_phone_udf = udf(mask_phone, StringType())
#
# # Apply UDFs to DataFrame
# masked_df = df.withColumn("masked_email", mask_email_udf(df.Mail)) \
# 	.withColumn("masked_phone", mask_phone_udf(df.mob))
#
# # Show the result
# masked_df.select("masked_email", "masked_phone").show(truncate=False)


#
# email = r("^[\w-\.]+@([\w-]+\.)+[\w-]{2,4}$")
#
# phone =  r("^\\d{10}$")







# note: scenario -9 given the id, contact details in the form of a string. you have to extract the
#  phone number, and email from the string if given else fill it with NULL
# data = [
# 	("E001", "John works at ABC Corp. Contact: 9876543210"),
# 	("E002", "Anna's email is anna.smith@gmail.com. Her phone number is 9123456789"),
# 	("E003", "No contact information available."),
# 	("E004", "Reach me at  or via mail alice.johnson@xyz.co.uk")
# ]
#
# # Define columns
# columns = ["employee_id", "contact_details"]
#
# # Create DataFrame
# df = spark.createDataFrame(data, columns)
#
# # Extract phone number (assumed to be 10 digits)
# phone_pattern = r'(\d{10})'
# email_pattern = r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})'
#
# df = (df
# 	  .withColumn(
# 		"phone_number",
# 			when(
# 					trim(
# 									regexp_extract(col("contact_details"),
# 							phone_pattern, 1)) == "",
# 									None)
# 					.otherwise(
# 							regexp_extract(
# 									col("contact_details"),
# 									phone_pattern,
# 										1)))
# 	  .withColumn(
# 		"email_id",
# 			when(
# 					trim(
# 								regexp_extract(col("contact_details"),
# 											email_pattern, 1)) == "",
# 											None)
# 					.otherwise(
# 							regexp_extract(
# 										col("contact_details"),
# 										email_pattern,
# 										1)))
# 	.select("employee_id", "phone_number", "email_id"))
# # Show the result
# df.show(truncate=False)



# note : 10
#  Given below is the stock data set which shows stock and its predicted price over next 7 working
#  days/sessions As a end user i want to know what should be my buy price and what should be my
#  sell price for each share so that i can earn maximum profit.
# E.g
# For RIL, if i buy it on first session and sell it on 4th session i will get 200 profit.

# stock_data = [
# 	("RIL", [1000, 1005, 1090, 1200, 1000, 900, 890]),
# 	("HDFC", [890, 940, 810, 730, 735, 960, 980]),
# 	("INFY", [1001, 902, 1000, 990, 1230, 1100, 1200])
# ]
# # Create DataFrame for the stock data
# columns = ["StockId", "PredictedPrice"]
# df = spark.createDataFrame(stock_data, columns)
# df.createOrReplaceTempView("stocks")
#
#
# # Explode the PredictedPrice array
# exploded_df = spark.sql("""
#     SELECT StockId, session, price
#     FROM (
#         SELECT StockId, posexplode(PredictedPrice) AS (session, price)
#         FROM stocks
#     )
# """)
#
# # Create a temporary view of the exploded DataFrame
# exploded_df.createOrReplaceTempView("exploded_stocks")
#
# # SQL to calculate the maximum profit
# result_sql = spark.sql("""
#     SELECT StockId,
#            buy_price,
#            sell_price,
#            (sell_price - buy_price) AS max_profit
#     FROM (
#         SELECT a.StockId,
#                a.price AS buy_price,
#                b.price AS sell_price,
#                ROW_NUMBER() OVER (PARTITION BY a.StockId ORDER BY (b.price - a.price) DESC) AS profit_rank
#         FROM exploded_stocks a
#         JOIN exploded_stocks b
#         ON a.StockId = b.StockId AND a.session < b.session
#     )
#     WHERE profit_rank = 1
#     order by max_profit desc
# """)
#
# result_sql.show()
#
# print("dsaf;lkjasd as;lkdjfasdlfkjlaksjlkjaflkjhasdfkljhasdfkjhasdlkjhasdflkjhasdfkjhasdfkjhasdkf jhasdfkjhsa fkjhasf")
#
# exploded_df = df.select(
# 	"StockId",
# 	posexplode("PredictedPrice").alias("session", "price")
# )
# # Calculate maximum profit using DataFrame API
# window_spec = Window.partitionBy("a.StockId").orderBy((col("b.price") - col("a.price")).desc())
#
# # Calculate maximum profit using DataFrame API
# results = (
# 	exploded_df.alias("a")
# 	.join(exploded_df.alias("b"), (col("a.StockId") == col("b.StockId")) & (col("a.session") < col("b.session")))
#
# )
# results.show()
#
# # Define a function to find the buy price, sell price, and max profit for each stock
# def calculate_max_profit(predicted_prices):
# 	prices = predicted_prices
# 	n = len(prices)
#
# 	# Initialize variables for tracking the minimum price and max profit
# 	min_price = prices[0]
# 	max_profit = 0
# 	buy_price = min_price
# 	sell_price = min_price
#
# 	# Iterate over the prices to find the optimal buy and sell points
# 	for i in range(1, n):
# 		if prices[i] < min_price:
# 			min_price = prices[i]
# 		elif prices[i] - min_price > max_profit:
# 			max_profit = prices[i] - min_price
# 			buy_price = min_price
# 			sell_price = prices[i]
#
# 	return buy_price, sell_price, max_profit
#
# # Register the function as a UDF
# calculate_max_profit_udf = udf(calculate_max_profit, StructType([
# 	StructField("BuyPrice", IntegerType(), True),
# 	StructField("SellPrice", IntegerType(), True),
# 	StructField("MaxProfit", IntegerType(), True)
# ]))
#
# # Apply the UDF on the DataFrame to calculate buy price, sell price, and profit
# df_with_profit = df.withColumn("ProfitDetails", calculate_max_profit_udf(col("PredictedPrice")))
#
# # Extract the BuyPrice, SellPrice, and MaxProfit from the struct column
# df_final = df_with_profit.select(
# 	"StockId",
# 	col("ProfitDetails.BuyPrice").alias("BuyPrice"),
# 	col("ProfitDetails.SellPrice").alias("SellPrice"),
# 	col("ProfitDetails.MaxProfit").alias("MaxProfit")
# )
#
# # Show the result
# df_final.show()
#







