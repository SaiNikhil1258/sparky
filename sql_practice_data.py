import os
import datetime
from datetime import timedelta
import urllib.request
from matplotlib.patheffects import withStroke
from matplotlib.pyplot import fill_between
from numpy.ma.core import inner
from pydantic.v1.utils import truncate
from pyparsing import withClass
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession, Window
import sys
from collections import namedtuple
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
import builtins
import random
import urllib.request

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['JAVA_HOME'] = r'C:\Users\LENOVO\.jdks\corretto-1.8.0_422'
os.environ["HADOOP_HOME"] = "C:\Program Files\Hadoop"

conf = (SparkConf()
		.setAppName("pyspark")
		.setMaster("local[*]")
		.set("spark.driver.host", "localhost")
		.set("spark.default.parallelism", "1")
		)
sc = SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()





#note: 1 Assume you're given a table Twitter tweet data, write a query to obtain a histogram of tweets posted per user in 2022.
# Output the tweet count per user as the bucket and the number of Twitter users who fall into that bucket.
# In other words, group the users by the number of tweets they posted in 2022 and count the number of users in each group.

# x = [
# ("214252",	"111","Am considering taking Tesla private at $420. Funding secured.",	    "12/30/2021 00:00:00"),
# ("739252",	"111","Despite the constant negative press covfefe",	                     "01/01/2022 00:00:00"),
# ("846402",	"111","Following @NickSinghTech on Twitter changed my life!",                "02/14/2022 00:00:00"),
# ("241425",	"254","If the salary is so competitive why won’t you tell me what it is?",   "03/01/2022 00:00:00"),
# ("231574",	"148","I no longer have a manager. I can't be managed",                      "03/23/2022 00:00:00"),
# ]
# df_daily_sales = spark.createDataFrame(x, ['tweet_id', 'user_id', 'msg', 'tweet_date'])
#
# df_daily_sales.show()
# df_daily_sales.printSchema()
# df_with_timestamp = (df_daily_sales
#                      .withColumn(
#                     "year",
#                         to_timestamp(df_daily_sales["tweet_date"], "MM/dd/yyyy HH:mm:ss"))
#                     )
# # filtering the  tweets in the year 2022
# df = df_with_timestamp.filter(year(df_with_timestamp["year"]) == 2022).groupby("user_id").agg(count("tweet_id").alias("tweet_count_per_user"))
# df.show()
#
# # grouping by the count of the tweets
# res = df.groupby("tweet_count_per_user").agg(count("user_id").alias("tweet_bucket"))
# res.show()


# note 2:Given a table of candidates and their skills, you're tasked with finding the candidates best
#  suited for an open Data Science job. You want to find candidates who are proficient in Python, Tableau, and PostgreSQL.
# Write a query to list the candidates who possess all of the required skills for the job.
# Sort the output by candidate ID in ascending order.

# columns = ["candidate_id",	"skill"]
# data = [
#     ("123",	"Python"),
#     ("123",	"Tableau"),
#     ("123",	"PostgreSQL"),
#     ("234",	"R"),
#     ("234",	"PowerBI"),
#     ("234",	"SQL Server"),
#     ("345",	"Python"),
#     ("345",	"Tableau"),
# ]
# df = spark.createDataFrame(data, columns)
# df.show()
# df.printSchema()
# # using dictionary to check for the skills needed
# required_skills_dict = {"Python", "Tableau", "PostgreSQL"}
# filtered_candidates = df.filter(df.skill.isin(required_skills_dict))
# filtered_candidates.show()
# candidates_with_skills = (filtered_candidates
#                           .groupBy("candidate_id")
#                           .agg(countDistinct("skill").alias("skill_count"))
#                           .filter(col("skill_count") == len(required_skills_dict))
#                           .orderBy("candidate_id"))
# candidates_with_skills.show()
#
# list_df=df.groupby("candidate_id").agg(sort_array(collect_list("skill")).alias("skills"))
# # when you use the array to compare there will be a datatype difference this below is the python list
# # not a java array list so we need to convert it into a datatype that spark can read using lit() method
# required_skills = sorted(["Python", "Tableau", "PostgreSQL"])
# required_skills_column = array([lit(skill) for skill in required_skills])
# list_df.show()
# print(required_skills_column)
# res = list_df.filter(list_df["skills"] == required_skills_column)
# res.show()
#
# df.createOrReplaceTempView("candidates")
# spark.sql("""
# SELECT
#     candidate_id
# FROM
#     candidates
# WHERE
#     skill IN ('Python', 'Tableau', 'PostgreSQL')
# GROUP BY
#     candidate_id
# HAVING
#     COUNT(skill) >= 3
# ORDER BY
#     candidate_id;
#
# """).show()






# note: 3 Assume you're given two tables containing data about Facebook Pages and their respective likes (as in "Like a Facebook Page").
# Write a query to return the IDs of the Facebook pages that have zero likes. The output should be sorted in ascending order based on the page IDs.

# columns1 = ["page_id",	"page_name"]
# data1 = [
#     ("20001",	"SQL Solutions"),
#     ("20045",	"Brain Exercises"),
#     ("20701",	"Tips for Data Analysts")
# ]
# columns2=["user_id",	"page_id",	"liked_date"]
# data2= [
#     ("111",	"20001",	"04/08/2022 00:00:00"),
#     ("121",	"20045",	"03/12/2022 00:00:00"),
#     ("156",	"20001",	"07/25/2022 00:00:00")
# ]
#
# df1 = spark.createDataFrame(data1, columns1)
# df2 = spark.createDataFrame(data2, columns2)
# df1.show()
# df2.show()
#
# df1.createOrReplaceTempView("df1")
# df2.createOrReplaceTempView("df2")
# spark.sql("""
#     select
#         p.page_id,
#         p.page_name
#     from
#         df1 p
#     left anti join
#         df2 l
#     on
#         p.page_id=l.page_id
#     order by
#         page_id
# """).show()
# # df1.join(df2, "page_id", "left_anti" ).show()
# df1.join(df2, "page_id", "anti" ).orderBy("page_id").show()




#note 4: Tesla is investigating production bottlenecks and they need your help to extract the relevant data.
# Write a query to determine which parts have begun the assembly process but are not yet finished.
# Assumptions:
# parts_assembly table contains all parts currently in production, each at varying stages of the assembly process.
# An unfinished part is one that lacks a finish_date.
# This question is straightforward, so let's approach it with simplicity in both thinking and solution.
# Effective April 11th 2023, the problem statement and assumptions were updated to enhance clarity.

# columns =["part",	"finish_date",	"assembly_step"]
# data=[
#     ("battery",	"01/22/2022 00:00:00","1"),
#     ("battery",	"02/22/2022 00:00:00","2"),
#     ("battery",	"03/22/2022 00:00:00","3"),
#     ("bumper",   "01/22/2022 00:00:00","1"),
#     ("bumper",   "02/22/2022 00:00:00","2"),
#     ("bumper",None,"3"),
#     ("bumper",None,"4"),
#  ]
#
# df = spark.createDataFrame(data, columns)
#
# df.show()
#
# df.filter(df["finish_date"].isNull()).show()
#
# df.createOrReplaceTempView("df")
# spark.sql("""
#     select
#         part,
#         assembly_step
#     from
#         df
#     where
#         finish_date is NULL
# """).show()


#note: 5 This is the same question as problem #3 in the SQL Chapter of Ace the Data Science Interview!
# Assume you're given the table on user viewership categorised by device type where the three types are laptop, tablet, and phone.
# Write a query that calculates the total viewership for laptops and mobile devices where mobile is defined as
# the sum of tablet and phone viewership. Output the total viewership for laptops as laptop_reviews and the total
# viewership for mobile devices as mobile_views.
# Effective 15 April 2023, the solution has been updated with a more concise and easy-to-understand approach.

# columns = ["user_id",	"device_type","view_time"]
# data = [
#     ("123",	"tablet",	"01/02/2022 00:00:00"),
#     ("125",	"laptop",	"01/07/2022 00:00:0"),
#     ("128",	"laptop",	"02/09/2022 00:00:0"),
#     ("129",	"phone",	"02/09/2022 00:00:0"),
#     ("145",	"tablet",	"02/24/2022 00:00:0"),
# ]
#
# df = spark.createDataFrame(data, columns)
#
# df.createOrReplaceTempView("df")
# spark.sql("""
#     SELECT
#         sum(case when device_type='laptop' then 1 else 0 end) as laptop_views,
#         sum(case when device_type in ('tablet', 'phone') then 1 else 0 end) as mobile_view
#     from df;
# """).show()
#
# df.agg(
#     sum(when(col("device_type")=='laptop',1).otherwise(0)).alias("laptop_views"),
#     sum(when(col("device_type").isin("tablet", "phone"),1).otherwise(0)).alias("mobile_views")
# ).show()





# note:6  Given a table of Facebook posts, for each user who posted at least twice in 2021, write a
#  query to find the number of days between each user’s first post of the year and last post of the year
#  in the year 2021. Output the user and number of the days between each user's first and last post.



# columns = ["user_id",	"post_id",	"post_content",	"post_date"]
# data = [
#     ("151652",	"599415",	"Need a hug",	"07/10/2021 12:00:00"),
#     ("661093",	"624356",	"Bed. Class 8-12. Work 12-3. Gym 3-5 or 6. Then class 6-10. Another day that's gonna fly by. I miss my girlfriend",	"07/29/2021 13:00:00"),
#     ("004239",	"784254",	"Happy 4th of July!",	"07/04/2021 11:00:00"),
#     ("661093",	"442560",	"Just going to cry myself to sleep after watching Marley and Me.",	"07/08/2021 14:00:00"),
#     ("151652",	"111766",	"I'm so done with covid - need travelling ASAP!",	"07/12/2021 19:00:00")
# ]
#
# df = spark.createDataFrame(data, columns)
# df = df.withColumn("post_date", to_timestamp("post_date", "MM/dd/yyyy HH:mm:ss"))
# df.createOrReplaceTempView("df")
# spark.sql("""
#     select
#         user_id,
#         date_diff(max(post_date),min(post_date)) as days_different
#     from
#         df
#     where
#         year(post_date)=2021
#     group by
#         user_id
#     having
#         count(post_id)>1
#     order by
#         days_different desc
# """).show()
#
# df.filter(year('post_date')==2021).groupBy("user_id").agg(date_diff(max("post_date"),min("post_date")).alias("days_difference")).filter("count(post_id)>1").orderBy(col("days_difference").desc()).show()







# note: 7 Write a query to identify the top 2 Power Users who sent the highest number of messages on
#  Microsoft Teams in August 2022. Display the IDs of these 2 users along with the total number of messages
#  they sent. Output the results in descending order based on the count of the messages.
# Assumption:
# No two users have sent the same number of messages in August 2022.
# find the top 2 senders in the month of august 2022

# columns=["message_id",	"sender_id",	"receiver_id",	"content",	"sent_date"]
# data = [
#     ("901",	"3601",	"4500",	"You up?",	"08/03/2022 00:00:00"),
#     ("902",	"4500",	"3601",	"Only if you're buying",	"08/03/2022 00:00:00"),
#     ("743",	"3601",	"8752",	"Let's take this offline",	"06/14/2022 00:00:00"),
#     ("922",	"3601",	"4500",	"Get on the call",	"08/10/2022 00:00:00"),
# ]
#
# rdd = spark.sparkContext.parallelize(data)
# df = rdd.toDF(columns)
# # df = spark.createDataFrame(data, columns)
# df = df.withColumn("sent_date", to_timestamp("sent_date", "MM/dd/yyyy HH:mm:ss"))
# df.createOrReplaceTempView("df")
# # df.show()
# # df.printSchema()
#
# # Perform filtering, grouping, counting, ordering, and limiting
# df.filter((year('sent_date')==2022)&(month('sent_date')==8)).groupBy("sender_id").count().orderBy(col("count").desc()).limit(2).show()
#
# # df.filter(year('sent_date')==2022) & (month('sent_date')==8)).groupBy("sender_id").count().orderBy(col("count").desc()).limit(2).show()
# spark.sql("""
#     select
#         sender_id,
#         count(*) as count
#     from
#         df
#     where
#        year(sent_date)=2022
#        and
#        month(sent_date)=8
#     group by
#         sender_id
#     order by
#         count desc
# """).show()






# note: 8 This is the same question as problem #8 in the SQL Chapter of Ace the Data Science Interview!
# Assume you're given a table containing job postings from various companies on the LinkedIn platform. Write a query to
# retrieve the count of companies that have posted duplicate job listings.
# Definition:
# Duplicate job listings are defined as two job listings within the same company that share identical titles and descriptions.

# columns = ["job_id",	"company_id",	"title",	"description"]
#
# data = [
# ("248",	"827",	"Business Analyst",	"Business analyst evaluates past and current business data with the primary goal of improving decision-making processes within organizations."),
# ("149",	"845",  "Business Analyst",	"Business analyst evaluates past and current business data with the primary goal of improving decision-making processes within organizations."),
# ("945",	"345",	"Data Analyst",     "Data analyst reviews data to identify key insights into a business's customers and ways the data can be used to solve problems."),
# ("164",	"345",	"Data Analyst",     "Data analyst reviews data to identify key insights into a business's customers and ways the data can be used to solve problems."),
# ("172",	"244",  "Data Engineer",    "Data engineer works in a variety of settings to build systems that collect, manage, and convert raw data into usable information for data scientists and business analysts to interpret."),
# ]
#
# rdd = sc.parallelize(data)
# df = rdd.toDF(columns)
# df.show()
# df.createOrReplaceTempView("df")
# spark.sql("""
#     WITH job_count_cte AS (
#           SELECT
#                 company_id,
#                 title,
#                 description,
#                 COUNT(job_id) AS job_count
#           FROM df
#           GROUP BY company_id, title, description
#     )
#     SELECT COUNT(DISTINCT company_id) AS duplicate_companies
#     FROM job_count_cte
#     WHERE job_count > 1;
# """).show()
#
#
# spark.sql("""
# SELECT COUNT(DISTINCT company_id) AS duplicate_company_count
# FROM (
#     SELECT company_id, COUNT(*) AS cnt
#     FROM df
#     GROUP BY company_id, title, description
#     HAVING cnt > 1
# )
# """).show()
#
# df.groupby("company_id", "title", "description").agg(count("*").alias("count")).filter("count>1").select("company_id").distinct().show()


# note: 9 This is the same question as problem #2 in the SQL Chapter of Ace the Data Science Interview!
#  Assume you're given the tables containing completed trade orders and user details in a Robinhood trading system.
#  Write a query to retrieve the top three cities that have the highest number of completed trade orders
#  listed in descending order. Output the city name and the corresponding number of completed trade orders.

# columns1=["order_id",	"user_id",	"quantity",	"status",	"date",	"price"]
# data1=[
# 	("100101",	"111",	"10",	"Cancelled",	"08/17/2022 12:00:00",	"9.80"),
# 	("100102",	"111",  "10","Completed",	"08/17/2022 12:00:00",	"10.00"),
# 	("100259",	"148",  "35","Completed","08/25/2022 12:00:00",	"5.10"),
# 	("100264",	"148",  "40","Completed","08/26/2022 12:00:00","4.80"),
# 	("100305",	"300",  "15","Completed","09/05/2022 12:00:00","10.00"),
# 	("100400",	"178",  "32","Completed","09/17/2022 12:00:00","12.00"),
# 	("100565",	"265",  "2", "Completed","09/27/2022 12:00:00","8.70"),
# ]
#
#
# columns2 = ["user_id",	"city",	"email",	"signup_date"]
# data2 = [
# 	("111",	"San Francisco",	"rrok10@gmail.com",	"08/03/2021 12:00:00"),
# 	("148",	"Boston",	"sailor9820@gmail.com",	"08/20/2021 12:00:00"),
# 	("178",	"San Francisco",	"harrypotterfan182@gmail.com",	"01/05/2022 12:00:00"),
# 	("265",	"Denver",	"shadower_@hotmail.com",	"02/26/2022 12:00:00"),
# 	("300",	"San Francisco",	"houstoncowboy1122@hotmail.com",	"06/30/2022 12:00:00"),
# ]
# rdd1 = sc.parallelize(data1)
# rdd2 = sc.parallelize(data2)
# df1 = rdd1.toDF(columns1)
# df2 = rdd2.toDF(columns2)
#
# df1.show()
# df2.show()
#
# df1.createOrReplaceTempView("df1")
# df2.createOrReplaceTempView("df2")
# spark.sql("""
#         select
#         	city,
# 			count(*) as count_orders
#         from
#             df1
#         join
#             df2
#         on
#             df1.user_id = df2.user_id
#         where
#         	df1.status='Completed'
#         group by
#         	city
#         order by
#         	count_orders desc
#         limit
#         	3
# """).show()
#
# result_df = df1.join(df2, "user_id" ) \
# 	.filter(df1["status"] == 'Completed') \
# 	.groupBy("city") \
# 	.agg(count("*").alias("count_orders")) \
# 	.orderBy(col("count_orders").desc()) \
# 	.limit(3)
#
# # Show the result
# result_df.show()






#note: 10  Given the reviews table, write a query to retrieve the average star rating for each product, grouped by
# month. The output should display the month as a numerical value, product ID, and average star rating
# rounded to two decimal places. Sort the output first by month and then by product ID.


# columns= [ "review_id",	"user_id",	"submit_date",	"product_id",	"stars" ]
# data = [
# 	( "6171",	"123",	"06/08/2022 00:00:00",	"50001",	"4" ),
# 	( "7802",	"265",	"06/10/2022 00:00:00",	"69852","4" ),
# 	( "5293",	"362",	"06/18/2022 00:00:00",	"50001","3" ),
# 	( "6352",	"192",	"07/26/2022 00:00:00",	"69852","3"),
# 	( '4517',	"981",	"07/05/2022 00:00:00",	"69852","2" ),
# ]
#
# rdd = sc.parallelize(data)
# df = rdd.toDF(columns)
# df = df.withColumn("submit_date", to_timestamp("submit_date", "MM/dd/yyyy HH:mm:ss"))
# df.show()
# df.printSchema()
# df.createOrReplaceTempView("df")
# spark.sql("""
# 	select
# 		month(submit_date) as month,
# 		product_id,
# 		avg(stars) as avg
# 	from
# 		df
# 	group by
# 		month(submit_date),
# 		product_id
# 	order by
# 		month,
# 		product_id
# """).show()
#
# x = (df
# 	 .withColumn("month", month("submit_date"))
# 	 .groupBy("month", "product_id")
# 	 .agg(avg("stars").alias("avg"))
# 	.withColumn("avg", round("avg",2))
# 	 .orderBy("month", "product_id")
# 	 )
# x.show()


# note: 11 This is the same question as problem #1 in the SQL Chapter of Ace the Data Science Interview!
# Assume you have an events table on Facebook app analytics. Write a query to calculate the click-through rate (CTR)
# for the app in 2022 and round the results to 2 decimal places.
# Definition and
# note:Percentage of click-through rate (CTR) = 100.0 * Number of clicks / Number of impressions
# To avoid integer division, multiply the CTR by 100.0, not 100.

#  columns = [ "app_id",	"event_type",	"timestamp" ]
# data = [
# 	( "123",	"impression",	"07/18/2022 11:36:12" ),
# 	( "123",	"impression",	"07/18/2022 11:37:12" ),
# 	( "123",	"click",	"07/18/2022 11:37:42" ),
# 	( "234",	"impression",	"07/18/2022 14:15:12" ),
# 	( "234",	"click",	"07/18/2022 14:16:12" ),
# ]
#
# rdd = sc.parallelize(data)
# df = rdd.toDF(columns)
# df = df.withColumn("timestamp", to_timestamp("timestamp", "MM/dd/yyyy HH:mm:ss"))
# df.printSchema()
# df.show()
#
# df.groupBy("app_id").agg(100.0 *
# 	sum(when(col("event_type")=='click',1).otherwise(0)).alias("clicks") /
# 	sum(when(col("event_type")=='impression',1).otherwise(0)).alias("impressions")
# ).alias("sum").show()



#note: 12 Assume you're given tables with information about TikTok user sign-ups and confirmations through email and text. New users on TikTok sign up using their email addresses, and upon sign-up, each user receives a text message confirmation to activate their account.
# Write a query to display the user IDs of those who did not confirm their sign-up on the first day, but confirmed on the second day.
# Definition:
# action_date refers to the date when users activated their accounts and confirmed their sign-up through text messages.

# columns1 = [ "email_id",	"user_id",	"signup_date"]
# data = [
# 	( "125",	"7771",	"06/14/2022 00:00:00" ),
# 	( "433",	"1052",	"07/09/2022 00:00:00" ),
# ]
#
# cols2=[ "text_id",	"email_id",	"signup_action",	"action_date" ]
# data2 = [
# 	( "6878",	"125",	"Confirmed",	"06/14/2022 00:00:00" ),
# 	( "6997",	"433","Not Confirmed","07/09/2022 00:00:00" ),
# 	( "7000",	"433","Confirmed","07/10/2022 00:00:00" ),
# ]
# rdd1= sc.parallelize( data )
# rdd2= sc.parallelize( data2 )
# df1 = rdd1.toDF(columns1)
# df2 = rdd2.toDF(cols2)
#
# df1 =df1.withColumn("signup_date", to_timestamp("signup_date", "MM/dd/yyyy HH:mm:ss"))
# df2=df2.withColumn("action_date", to_timestamp("action_date", "MM/dd/yyyy HH:mm:ss"))
# df1.createOrReplaceTempView("emails")
# df2.createOrReplaceTempView("texts")
# df1.show()
# df1.printSchema()
# df2.show()
# df2.printSchema()
# spark.sql("""
# 	SELECT
# 		DISTINCT emails.user_id
# 	FROM
# 		emails
# 	INNER JOIN
# 		texts
# 	ON
# 		emails.email_id = texts.email_id
# 	WHERE
# 		DATE(texts.action_date) = DATE_ADD(DATE(emails.signup_date), 1)
# 		AND
# 		texts.signup_action = 'Confirmed';
# """).show()
#
# res = (df1
# 	   .join(df2, "email_id", "inner")
# 	   .filter(
# 			(df2.action_date == date_add(df1.signup_date, 1))
# 				&
# 			(df2.signup_action=='Confirmed')
# 		)
# 	   .select("user_id")
# 	   .distinct())
# res.show()

#note: 13 IBM is analyzing how their employees are utilizing the Db2 database by tracking the SQL queries
# executed by their employees. The objective is to generate data to populate a histogram that shows
# the number of unique queries run by employees during the third quarter of 2023 (July to September).
# Additionally, it should count the number of employees who did not run any queries during this period.
# Display the number of unique queries as histogram categories, along with the count of employees who
# executed that number of unique queries.
# need some iterations on the query need to figure out for the dsl and the spark sql

# cols1 = [ "employee_id",	"query_id",	"query_starttime",	"execution_time" ]
# data1 = [
# 	( "3",	"856987",	"07/01/2023 03:25:12",	"2698" ),
# 	( "3",	"286115",	"07/01/2023 04:34:38",	"2705" ),
# 	( "3",	"33683","07/02/2023 10:55:14",	"91" ),
# 	( "1",	"413477","07/15/2023 11:35:09",	"470" ),
# 	( "1", "421983", "07/01/2023 14:33:47", "3020" ),
# 	( "2",	"17745","07/01/2023 14:33:47",	"2093" ),
# 	( "2", "958745", "07/02/2023 08:11:45", "512" ),
# 	( "2", "684293", "07/22/2023 18:42:31", "1630" ),
# 	( "2", "385739", "07/25/2023 14:25:17", "240" ),
# 	( "2", "123456", "07/26/2023 16:12:18", "950" ),
# ]
#
#
# cols2 = [ "employee_id",	"full_name",	"gender" ]
# data2 = [
# 	( "1",	"Judas Beardon",	"Male" ),
# 	( "2",	"Lainey Franciotti",	"Female" ),
# 	( "3",	"Ashbey Strahan",	"Male" ),
# ]
# rdd1 = sc.parallelize(data1)
# rdd2 = sc.parallelize(data2)
# df1 = rdd1.toDF(cols1)
# df2 = rdd2.toDF(cols2)
# df1 = df1.withColumn("query_starttime", to_timestamp("query_starttime","MM/dd/yyyy HH:mm:ss"))
# df2.createOrReplaceTempView("employees")
# df1.createOrReplaceTempView("queries")
# spark.sql("""
# WITH employee_queries AS (
#   SELECT
#     e.employee_id,
#     COALESCE(COUNT(DISTINCT q.query_id), 0) AS unique_queries
#   FROM employees AS e
#   LEFT JOIN queries AS q
#     ON e.employee_id = q.employee_id
#       AND q.query_starttime >= '2023-07-01T00:00:00Z'
#       AND q.query_starttime < '2023-10-01T00:00:00Z'
#   GROUP BY e.employee_id
# )
#
# SELECT
#   unique_queries,
#   COUNT(employee_id) AS employee_count
# FROM employee_queries
# GROUP BY unique_queries
# ORDER BY unique_queries;
# """).show()
#
#
# filtered_queries = df1.filter(
# 	(col("query_starttime") >= "2023-07-01") &
# 	(col("query_starttime") < "2023-10-01")
# )
#
# # Step 2: Perform the left join between employees and filtered queries
# joined_df = df2.join(
# 	filtered_queries,
# 	df2["employee_id"] == filtered_queries["employee_id"],
# 	how="left"
# )
#
# # Step 3: Group by employee_id and count distinct query_id
# employee_queries = joined_df.groupBy(df2["employee_id"]).agg(
# 	coalesce(countDistinct("query_id"), lit(0)).alias("unique_queries")
# )
#
# # Step 4: Group by unique_queries and count the number of employees in each category
# result = employee_queries.groupBy("unique_queries").agg(
# 	count("employee_id").alias("employee_count")
# ).orderBy("unique_queries")
#
# # Show the result
# result.show()
#




#note: 14 Your team at JPMorgan Chase is preparing to launch a new credit card, and to gain some insights,
# you're analyzing how many credit cards were issued each month.
# Write a query that outputs the name of each credit card and the difference in the number of issued
# cards between the month with the highest issuance cards and the lowest issuance. Arrange the results
# based on the largest disparity.

# cols = ["card_name",	"issued_amount",	"issue_month",	"issue_year" ]
# data = [
# 	 ( "Chase Freedom Flex",	"55000",	"1",	"2021" ),
# 	 ( "Chase Freedom Flex","60000",	"2",	"2021" ),
# 	 ( "Chase Freedom Flex","65000",	"3",	"2021" ),
# 	 ( "Chase Freedom Flex","70000","4",	"2021" ),
# 	 ( "Chase Sapphire Reserve",	"170000",	"1",	"2021" ),
# 	 ( "Chase Sapphire Reserve","175000",	"2",	"2021" ),
# 	 ( "Chase Sapphire Reserve","180000",	"3",	"2021" ),
# ]
#
#
# rdd = sc.parallelize(data)
# df = rdd.toDF(cols)
# df.show()
#
#
# window_spec = Window.partitionBy("card_name").orderBy("issue_month")
#
# res = (df
# 	   .groupBy("card_name")
# 	   .agg( (max("issued_amount")-min("issued_amount")).alias("difference"))
# 	   .withColumn("difference", floor("difference"))
# 	)
# res.show(truncate=False)


