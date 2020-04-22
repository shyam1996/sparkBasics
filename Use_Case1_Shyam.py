# Databricks notebook source
# MAGIC %md
# MAGIC 1. Read the file from S3 (__s3://data-engineer-training/data/card_transactions.json__)
# MAGIC 2. File has json records
# MAGIC 3. Each record has fields:
# MAGIC     * user_id
# MAGIC     * card_num
# MAGIC     * merchant
# MAGIC     * category
# MAGIC     * amount
# MAGIC     * ts

# COMMAND ----------

# MAGIC %md
# MAGIC For the one month (1st Feb to 29th Feb 2020: __1580515200 <= ts < 1583020800__), perform below analysis:
# MAGIC 1. Get the total amount spent by each user
# MAGIC 2. Get the total amount spent by each user for each of their cards
# MAGIC 3. Get the total amount spend by each user for each of their cards on each category
# MAGIC 4. Get the distinct list of categories in which the user has made expenditure
# MAGIC 5. Get the category in which the user has made the maximum expenditure

# COMMAND ----------

ACCESS_KEY  = 'AKIASE7ZJRTXHT3RCVY5'
SECRET_KEY  = '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'
BUCKET_NAME = 'data-engineer-training'
sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", ACCESS_KEY)
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", SECRET_KEY)

# COMMAND ----------

import json
rawRdd = sc.textFile("s3://data-engineer-training/data/card_transactions.json")
rdd = rawRdd.map(lambda line: json.loads(line))
#For the one month (1st Feb to 29th Feb 2020: 1580515200 <= ts < 1583020800), so to filter we use
monthRdd = rdd.filter(lambda x: 1580515200 <= x['ts'] < 1583020800)
monthRdd.cache()

# COMMAND ----------

monthRdd.collect()

# COMMAND ----------

def userAmount(inp):
  # returns (user_id, Amount) as a tuple (pair Rdd)
  #Key : user_id
  #Value : Amount
  return (inp['user_id'], inp['amount'])

#Get the total amount spent by each user
userAmount_Rdd = monthRdd.map(userAmount)
totalAmount_User = userAmount_Rdd.reduceByKey(lambda x,y: x+y)
totalAmount_User.collect()

# COMMAND ----------

def userCardAmount(inp):
  # returns ((user_id, card_num), Amount) as a tuple (pair Rdd)
  #Key : (user_id, card_num)
  #Value: Amount
  return ((inp['user_id'], inp['card_num']), inp['amount'])

#Get the total amount spent by each user for each of their cards
UCardAmount_Rdd = monthRdd.map(userCardAmount)
totalAmount_UCard = UCardAmount_Rdd.reduceByKey(lambda x,y: x+y)
totalAmount_UCard.collect()

# COMMAND ----------

def UCCAmount(inp):
  # returns ((user_id, card_num, category) Amount) as a tuple (pair Rdd)
  #Key : (user_id, card_num, category)
  #Value: Amount
  return ((inp['user_id'], inp['card_num'], inp['category']), inp['amount'])

#Get the total amount spend by each user for each of their cards on each category
UCCategoryAmount_Rdd = monthRdd.map(UCCAmount)
totalAmount_PUCCategory = UCCategoryAmount_Rdd.reduceByKey(lambda x,y: x+y)
totalAmount_PUCCategory.collect()

# COMMAND ----------

def Ucategory(inp):
  # returns (user_id, category) as a tuple (pair Rdd)
  #Key : user_id
  #Value: category
  return (inp['user_id'], inp['category'])

def merge_values(inp, x):
  inp.add(x)
  return inp

def merge_combiners(inp1, inp2):
  inp1.update(inp2)
  return inp1
  
#Get the distinct list of categories in which the user has made expenditure
Ucategory_Rdd = monthRdd.map(Ucategory)
Ucategories = Ucategory_Rdd.combineByKey(lambda x: set(), merge_values, merge_combiners)
Ucategories.collect()

# COMMAND ----------

def UCAmount(inp):
  # returns ((user_id, category), Amount) as a tuple (pair Rdd)
  #Key : (user_id, category)
  #Value: Amount
  return ((inp['user_id'], (inp['category'])), inp['amount'])

#Get the category in which the user has made the maximum expenditure
UCAmount_Rdd = monthRdd.map(UCAmount)
UCAmount = UCAmount_Rdd.reduceByKey(lambda x,y: x+y)
UCAmount2 = UCAmount.map(lambda x: (x[0][0], (x[0][1], x[1]))).groupByKey()
UCAmount3 = UCAmount2.map(lambda x :(x[0],(sorted(x[1], key=lambda x: x[1], reverse=True))[0]))
UCAmount3.collect()

# COMMAND ----------

#extra for last question
from collections import Counter

def UCAmount(inp):
  return (inp['user_id'], (inp['category'], inp['amount']))

def merge_values2(inp, x):
  if x[0] in inp:
    inp[x[0]] += x[1]
  else:
    inp[x[0]] = x[1]
  return inp

def merge_combiners2(inp1, inp2):
  inp1 = dict(Counter(inp1) + Counter(inp2))
  return inp1

#Get the category in which the user has made the maximum expenditure
UCAmountRdd = monthRdd.map(UCAmount)
Ucategories = UCAmountRdd.combineByKey(lambda x: dict([x]), merge_values2, merge_combiners2)
Ucategories.collect()
