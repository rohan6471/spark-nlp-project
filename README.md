# Natural Language Processing Using Spark with Python

## Nature of data
The processing is been done on raw text data retrived from one of the popular books from project Gutenberg. This text consists much of messy data including non-letter characters and stop words.

## Tools Used For Processing:
* Spark with Python
* Databricks Community Cloud 
 
 ## Steps to process the data:
- First Step is to retrieve the text from url using 'urllib' python library and then moving the file to databricks file system to process the data on cloud
 ``` 
 import urllib.request

urllib.request.urlretrieve("https://www.gutenberg.org/files/64317/64317-0.txt","/tmp/essay.txt")
dbutils.fs.mv("file:/tmp/essay.txt",'dbfs:/data/essay.txt')
 ```
 
- Next step is to read the data into Spark RDD using the reference of Spark Context 'sc' with defined number of partitions
``` 
num_partitions=4
rawRDD= sc.textFile('dbfs:/data/story.txt',num_partitions)
```
- Then using flatMap transformation, split each line into words removing the leading and trail spaces.
```
wordsRDD=rawRDD.flatMap(lambda line:line.strip().split(" "))
```
- Next remove non-letter characters from words RDD by defining a function and replace the non-letter characters with empty string using 're' python library and apply this function to words in RDD using map transformation.
```
import re
def removePunctuation(text):
    return re.sub('([^\w\s]|_)','',text,0).strip().lower()
non_letter_rdd=wordsRDD.map(lambda words: removePunctuation(words))
```
- Using StopWordsRemover from pyspark.ml.feature package, remove all the stop words from the rdd using filter transformation and then genearte a tuple with words and literal 1 using map transformation.
```
from pyspark.ml.feature import StopWordsRemover
remover=StopWordsRemover()
stopwords=remover.getStopWords()
cleanwordsRDD=non_letter_rdd.filter(lambda word: word not in stopwords).map(lambda word: (word,1))
```
- Finally, reduce all the intermediate values using 'reduceByKey' transformation and collect the results using collect action.
```
resultRDD=cleanwordsRDD.reduceByKey(lambda acc, value: acc+value)
results=resultRDD.collect()
```
# Visualizing the Top 10 words using Barplot

- Create a pandas dataframe using the results list and then filter the top 10 words from list.
```
df = pd.DataFrame.from_records(results, columns =[xlabel, ylabel])
df2=df.nlargest(10,["count"])
```
- Using 'matplotlib library of python plot the results using Horizontal bar  
```
fig, ax = plt.subplots(figsize=(8, 8))

# Plot horizontal bar graph
df2.sort_values(by='count').plot.barh(x='word',
                      y='count',
                      ax=ax,
                      color="lightblue")

ax.set_title("Top 20 Common Words Found in Text ")

plt.show()
```

![](https://github.com/rohan6471/spark-nlp-project/blob/main/finalimg.PNG)

# References:

- https://www.gutenberg.org/files/64317/64317-0.txt
- https://datascience-enthusiast.com/Python/cs110_lab3a_word_count_rdd.html

### Author 
   Rohan Goud
