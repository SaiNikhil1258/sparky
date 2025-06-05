import os
import urllib.request
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
import sys
from collections import namedtuple
from pyspark.sql.functions import *
from pyspark.sql.types import *

python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['JAVA_HOME'] = r'C:\Users\LENOVO\.jdks\corretto-1.8.0_422'

conf = (SparkConf().setAppName("pyspark").setMaster("local[*]").set("spark.driver.host", "localhost").set(
	"spark.default.parallelism", "1"))
sc = SparkContext(conf=conf)
spark = SparkSession.builder.getOrCreate()
import urllib.request

"""
SPARK BOILERPLATE
"""

data = [
	(0, "06-26-2011", 300.4, "Exercise", "GymnasticsPro", "cash"),
	(1, "05-26-2011", 200.0, "Exercise Band", "Weightlifting", "credit"),
	(2, "06-01-2011", 300.4, "Exercise", "Gymnastics Pro", "cash"),
	(3, "06-05-2011", 100.0, "Gymnastics", "Rings", "credit"),
	(4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
	(5, "02-14-2011", 200.0, "Gymnastics", None, "cash"),
	(6, "06-05-2011", 100.0, "Exercise", "Rings", "credit"),
	(7, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
	(8, "02-14-2011", 200.0, "Gymnastics", None, "cash")
]

df = spark.createDataFrame(data, ["id", "tdate", "amount", "category", "product", "spendby"])
# df.show()

data2 = [
	(4, "12-17-2011", 300.0, "Team Sports", "Field", "cash"),
	(5, "02-14-2011", 200.0, "Gymnastics", None, "cash"),
	(6, "02-14-2011", 200.0, "Winter", None, "cash"),
	(7, "02-14-2011", 200.0, "Winter", None, "cash")
]

df1 = spark.createDataFrame(data2, ["id", "tdate", "amount", "category", "product", "spendby"])
# df1.show()

data4 = [
	(1, "raj"),
	(2, "ravi"),
	(3, "sai"),
	(5, "rani")
]

cust = spark.createDataFrame(data4, ["id", "name"])
# cust.show()

data3 = [
	(1, "mouse"),
	(3, "mobile"),
	(7, "laptop")
]

prod = spark.createDataFrame(data3, ["id", "product"])
# prod.show()

# Register DataFrames as temporary views
df.createOrReplaceTempView("df")
df1.createOrReplaceTempView("df1")
cust.createOrReplaceTempView("cust")
prod.createOrReplaceTempView("prod")



spark.sql("select id, tdate from df").show()
spark.sql("select * from df where category='Exercise'").show()
spark.sql("select id, tdate,category,spendby from df where category='Exercise' and spendby='cash'").show()
spark.sql("select * from df where category in ('Exercise','Gymnastics')").show()
spark.sql("select *  from df where product like '%Gymnastics%'").show()
spark.sql("select * from df where category!='Exercise'").show()
spark.sql("select * from df where category not in ('Exercise','Gymnastics')").show()
spark.sql("select * from df where product is null").show()
spark.sql("select * from df where product is not null").show()
spark.sql("select max(id) from df").show()
spark.sql("select min(amount) from df").show()
#NOTE: we can use * for  the count we can also pass 1 in place of *
spark.sql("select count(*) from df").show()
#NOTE:  when you need to add one extra column with  some extra conditionals then use "case when ... end "
# statements to get the result
spark.sql("select *, case when spendby='cash' then 1 when spendby='credit' then 0 else null end as status from df").show()
#NOTE: concatenation can be done for multipe columns also for that see the next concat_ws command
spark.sql("select id, category,concat(id,'-',category) as ConcatedData from df").show()
spark.sql("select id, category,concat_ws(',',id,category,product,spendby) as MUltipleColsConcatedData from df").show()
spark.sql("select id, category, lower(category) as lowerCase from df").show()
spark.sql("select id, category, upper(category) as upperCase from df").show()
spark.sql("select id, amount as beforeCeil, ceil(amount) as afterCeil from df").show()
spark.sql("select id, amount as beforeCeil, round(amount) as afterCeil from df").show()
#NOTE: coalesce is a function which will find all the NULL values in the given column or columns and then replace
# the value with the given default value
spark.sql("select id,product, coalesce(product,'NA') from df ").show()
# NOTE trim operation to remove all the white spaces in the prefix and suffix of the column data but not in between the data
spark.sql("select id, trim(category) from df").show()
spark.sql("select distinct(category) from df").show()
spark.sql("select id, substring(product,1,10) as substring_upto_10_chars from df").show()
spark.sql("select id, product, split(product,' ')[0] as split from df").show()
# INFO: UNIONS
spark.sql("select * from df union all select * from df1").show()
spark.sql("select category,sum(amount) as SUM  from df group by category").show()
spark.sql("select category,spendby,sum(amount) as SUM  from df group by category, spendby").show()
# NOTE: you can specify the column name instead of * to be specific or it doesn't matter
spark.sql("select category,spendby,count(*) as count,sum(amount) as SUM  from df group by category, spendby").show()
spark.sql("select category,max(amount) as Max from df group by category order by Max desc").show()


#NOTE: windowing functions
spark.sql("select id, category, amount, row_number()OVER(partition by category order by amount desc) as serialized_by_group from df").show()
# NOTE: dense rank will rank the duplicates serially
spark.sql("select id, category, amount, dense_rank()OVER(partition by category order by amount desc) as PlainRank from df").show()
# NOTE: rank will rank the duplicates the same number but the serial count is maintained if there are 2 duplicates
# out of the 3 columns the duplicates will have the same rank suppose 1 the other third column will have the rank as 3 not(2))
spark.sql("select id, category, amount, rank()OVER(partition by category order by amount desc) as DenseRank from df").show()



# NOTE: lead/ lag
# INFO: lead( col_name, lead_value, some value that you want to have else default(null))
spark.sql("select id, category, amount, lead(amount, 1,0) over(partition by category order by amount desc) as leadAmount from df").show()
spark.sql("select id, category, amount, lag(amount, 1,0) over(partition by category order by amount desc) as lagAmount from df").show()

#NOTE: having

spark.sql("select category, count(category) as count from df group by category having count(category)>2").show()



# NOTE: JOINS
# INNER JOIN
spark.sql("select a.*, b.product from cust a join prod b on a.id=b.id").show()

# LEFT JOIN (yeah left and right are both same just flip the columns)
spark.sql("select a.*, b.product from cust a left join prod b on a.id=b.id").show()

#FULL JOIN
spark.sql("select a.*, b.product from cust a full join prod b on a.id=b.id").show()

#ANTI JOIN
spark.sql("select a.* from cust a anti join prod b on a.id=b.id").show()

# Date format
spark.sql("select id, tdate, from_unixtime(unix_timestamp(tdate,'MM-dd-yyyy'),'yyyy-MM-dd') as con_date from df").show()







# print("df")
# df.show()
# print("df1")
# df1.show()
# print("cust")
# cust.show()
# print("prod")
# prod.show()