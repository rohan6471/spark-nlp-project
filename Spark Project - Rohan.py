# Databricks notebook source
import urllib.request

urllib.request.urlretrieve("https://www.gutenberg.org/files/64317/64317-0.txt","/tmp/essay.txt")
dbutils.fs.mv("file:/tmp/essay.txt",'dbfs:/data/essay.txt')


# COMMAND ----------

num_partitions=4
rawRDD= sc.textFile('dbfs:/data/story.txt',num_partitions)


# COMMAND ----------

from pyspark.ml.feature import StopWordsRemover
remover=StopWordsRemover()
stopwords=remover.getStopWords()
print(stopwords)

# COMMAND ----------

wordsRDD=rawRDD.flatMap(lambda line:line.strip().split(" "))


# COMMAND ----------

import re
def removePunctuation(text):
    return re.sub('([^\w\s]|_)','',text,0).strip().lower()
  

# COMMAND ----------

non_letter_rdd=wordsRDD.map(lambda words: removePunctuation(words))
cleanwordsRDD=rdd.filter(lambda word: word not in stopwords).map(lambda word: (word,1))

# COMMAND ----------

resultRDD=cleanwordsRDD.reduceByKey(lambda acc, value: acc+value)
results=resultRDD.collect()

# COMMAND ----------

print(results)

# COMMAND ----------

# MAGIC %py
# MAGIC import numpy as np
# MAGIC import pandas as pd
# MAGIC import matplotlib.pyplot as plt
# MAGIC import seaborn as sns
# MAGIC from collections import Counter
# MAGIC 
# MAGIC title = 'Top 10 Words Used in the book' 
# MAGIC xlabel = 'word'
# MAGIC ylabel = 'count'
# MAGIC 
# MAGIC # create Pandas dataframe from results list
# MAGIC df = pd.DataFrame.from_records(results, columns =[xlabel, ylabel])
# MAGIC df2=df.nlargest(10,["count"])
# MAGIC print(df2)
# MAGIC 
# MAGIC 
# MAGIC # create plot (using matplotlib)
# MAGIC plt.figure(figsize=(10,3))
# MAGIC sns.barplot(xlabel, ylabel, data=df2, palette="Blues_d").set_title(title)
