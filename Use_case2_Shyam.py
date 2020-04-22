# Databricks notebook source
# MAGIC %md
# MAGIC __Input__
# MAGIC 1. Configuration JSON file: (JSON file Consists of input file location, output file location, delimiter in the actual data file)
# MAGIC   - The configuration file is at: __s3://data-engineer-training/data/auto_loan.json__
# MAGIC 2. Auto Loan Datasets: 
# MAGIC It has following fields
# MAGIC   - application_id
# MAGIC   - customer_id
# MAGIC   - car_price
# MAGIC   - car_model
# MAGIC   - customer_location
# MAGIC   - request_date
# MAGIC   - loan_status
# MAGIC 
# MAGIC 3. The input location and delimiter is present in the configuration file (Hint: Use *sc.wholeTextFiles* to read the configuration file and then parse it using the json module)
# MAGIC 
# MAGIC 4. The first line of the file is the header

# COMMAND ----------

# MAGIC %md
# MAGIC __Perform following analysis__
# MAGIC 
# MAGIC 1. The month in which maximum loan requests were submitted in the last one year [2019-04-01 to 2020-03-31]
# MAGIC 
# MAGIC 2. Max, Min and Average number of applications submitted per customer id
# MAGIC 
# MAGIC 3. Top 10 highest car price against which applications got approved
# MAGIC 
# MAGIC 4. For each customer location, top 5 car models which have most loan applications in the last month

# COMMAND ----------

ACCESS_KEY  = 'AKIASE7ZJRTXHT3RCVY5'
SECRET_KEY  = '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'
BUCKET_NAME = 'data-engineer-training'
sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", ACCESS_KEY)
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", SECRET_KEY)

# COMMAND ----------

jsonPath = "s3://data-engineer-training/data/auto_loan.json"
jsonFile = spark.read.json(jsonPath, multiLine = True)
jsonFile.collect()

# COMMAND ----------

input_location = jsonFile.collect()[0]['input_location']
output_location = jsonFile.collect()[0]['output_location']
delimiter = jsonFile.collect()[0]['delimiter']

print("input location : {}\noutput_location : {}\ndelimiter : {}".format(input_location, output_location, delimiter))

# COMMAND ----------

auto_loan_df = spark.read.format("csv")\
  .option("header", "true")\
  .option("inferSchema", "true")\
  .load(input_location)
auto_loan_df.show(5)

# COMMAND ----------

auto_loan_df.createOrReplaceTempView("in_memory_retail_table")

# COMMAND ----------

#The month in which maximum loan requests were submitted in the last one year [2019-04-01 to 2020-03-31]

query1 = """SELECT
  DATE_FORMAT(request_date, 'YYYY-MM') as newd, count(application_id) as ad
FROM in_memory_retail_table
group by newd
order by ad DESC"""

spark.sql(query1).show(1)

# COMMAND ----------

#Max, Min and Average number of applications submitted per customer id

query2 = """SELECT max(cnt), min(cnt) from (select
  customer_id, count(application_id) as cnt
  from in_memory_retail_table
  group by customer_id) a
  """
queryx = """select (count( DISTINCT application_id )/count(DISTINCT customer_id)) as avg
from in_memory_retail_table"""
spark.sql(queryx).show()
spark.sql(query2).show()

# COMMAND ----------

#Top 10 highest car price against which applications got approved\\

query3 = """SELECT
  car_price, loan_status
FROM in_memory_retail_table
WHERE loan_status = 'approved'
order by car_price desc
limit 10
"""

spark.sql(query3).show()

# COMMAND ----------

#For each customer location, top 5 car models which have most loan applications in the last month

query4 = """ select * from 
(select *, row_number() OVER (PARTITION BY customer_location ORDER BY appCnt DESC) AS rank
from
(select
customer_location, car_model, count(application_id) as appCnt
from in_memory_retail_table
group by customer_location, car_model
order by customer_location, appCnt DESC
) b) c 
where rank < 6
"""

spark.sql(query4).show()
