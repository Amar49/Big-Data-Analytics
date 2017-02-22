###########################################
# Author: Aviral Nigam
# SBU ID: 110849584
###########################################

from pyspark import SparkContext
from operator import add

def is_ascii(s):
    return all(ord(c) < 128 for c in s)

# Creating Spark Context
sc = SparkContext(appName="Word Count")

# Getting the zip file from remote location
file_data = sc.textFile("s3://commoncrawl/crawl-data/CC-MAIN-2016-40/wet.paths.gz")

# Extracting the gzip file and getting first 5000 lines
urls = file_data.map(lambda line: "s3://commoncrawl/" + line)
files = urls.collect()
files = files[:1000]

print(files)

# Loading data from the s3 cluster for 5000 files
logData = sc.textFile(",".join(files), 6 * sc.defaultParallelism)

# Converting data to lower case
# Splitting around ' i feel ' and finding sentences starting with 'i feel ' or have ' i feel '
lines = logData.filter(lambda s: s.lower().startswith('i feel ') or ' i feel ' in s.lower()).map(lambda s: (' ' + s).lower().replace(" i feel i feel ", " i feel i i feel ").split(' i feel ')[1:])

# Map all words occuring after ' i feel '
parts = lines.flatMap(lambda l: l[:])

# Spliting the data from above by space and then extracting the word following ' i feel '
# Filter for ascii words
counts = parts.flatMap(lambda x: x.strip().split(' ')[:1]).filter(lambda x: is_ascii(x)).map(lambda x: (x, 1)).reduceByKey(add)

# Filtering for words whose frequency is greater than 30
word_count = counts.filter(lambda w: w[1] >= 30)

# Collecting results from all the RDDs
word_count.coalesce(1).saveAsTextFile("s3://my-wordcount/result-1000")

# Closing the spark context
sc.stop()