###########################################
# Author: Aviral Nigam
# SBU ID: 110849584
###########################################

import itertools
from pyspark import SparkContext
from operator import add
import sys

# Initialize the spark context
sc = SparkContext(appName="Frequent Itemset")

# Take filename from command line arguments
filename = str(sys.argv[1])

# Load file into spark
file = sc.textFile(filename)

# Extract the data and take only unique values
extract_data = file.map(lambda p : p.split(",")).map(lambda p : (p[1],p[0])).distinct()

# Filter to items associated with at least 10 users
user_filter_temp = extract_data.groupByKey().mapValues(lambda x: list(x)).filter(lambda x: len(x[1]) >= 10)

# Filter to users associated with at least 5 items
user_filter = user_filter_temp.flatMap(lambda x: [(item, x[0]) for item in x[1]]).groupByKey().mapValues(lambda x: list(x)).filter(lambda x: len(x[1]) >= 5)

# Take user count per item which will be used later for interest calculation
single_item_count = user_filter.flatMap(lambda x: [(item, x[0]) for item in x[1]]).groupByKey().mapValues(lambda x : list(x)).map(lambda x : (x[0],len(x[1])))

# Create pair of items and calculate their user list support to be greater than 10 
‪pair_items = user_filter.flatMap(lambda x : [(item, x[0]) for item in itertools.combinations(x[1], 2)]).distinct().groupByKey().mapValues(lambda x: list(x)).filter(lambda x : len(x[1]) >= 10)

# Calculate the number of item pairs for k=2
no_of_item_pairs = ‪pair_items.keys().count()

# Create triplets of pair items
‪pair_items_temp = ‪pair_items.flatMap(lambda x: [(item, x[0]) for item in x[1]]).groupByKey().mapValues(lambda x: list(x)).flatMap(lambda x : [(x[0], item) for item in itertools.combinations(x[1], 3)]).groupByKey().mapValues(lambda x: list(x))

# Unpersist old unsed RDD
‪pair_items.unpersist()

# Create triplet of items and calculate their user list support to be greater than 10 
triplet_items = ‪pair_items_temp.flatMap(lambda x : [(x[0], list(set(item[0]+item[1]+item[2]))) for item in x[1]]).filter(lambda x : len(x[1]) == 3).map(lambda x : (x[0],(x[1][0], x[1][1], x[1][2]))).groupByKey().mapValues(lambda x: list(x)).flatMap(lambda x: [(item, x[0]) for item in x[1]]).groupByKey().mapValues(lambda x: list(x)).filter(lambda x : len(x[1]) >= 10)

# Calculate to get item and no. of users to be used for confidence calculation
triplet_count = triplet_items.map(lambda x : (x[0], len(x[1])))

# Calculate the number of item triplets for k=3
no_of_item_triplets = triplet_items.keys().count()

# Create tuples of 4 items 
triplet_items_temp = triplet_items.flatMap(lambda x: [(item, x[0]) for item in x[1]]).groupByKey().mapValues(lambda x: list(x)).flatMap(lambda x : [(x[0], item) for item in itertools.combinations(x[1], 4)]).groupByKey().mapValues(lambda x: list(x))

# Unpersist old unsed RDD
triplet_items.unpersist()

# Calculate the user list support to be greater than 10
four_tuple_items_count = triplet_items_temp.flatMap(lambda x : [(x[0], list(set(item[0]+item[1]+item[2]+item[3]))) for item in x[1]]).filter(lambda x : len(x[1]) == 4).map(lambda x : (x[0],(x[1][0], x[1][1], x[1][2], x[1][3]))).groupByKey().mapValues(lambda x: list(x)).flatMap(lambda x: [(item, x[0]) for item in x[1]]).groupByKey().mapValues(lambda x: list(x)).filter(lambda x : len(x[1]) >= 10).map(lambda x : (x[0], len(x[1])))

# Check for confidence of the fourth item being added to the tuple to be greater than equal to 0.05
confidence = four_tuple_items_count.cartesian(triplet_count).filter(lambda x : x[0][0][0] in x[1][0] and x[0][0][1] in x[1][0] and x[0][0][2] in x[1][0]).map(lambda x : ((x[0][0], x[1][0]), (float(x[0][1]) / float(x[1][1])))).filter(lambda x : x != None and x[1] >= 0.05).distinct()

# Calculate total no. of users for interest calculation
total_no_of_users = user_filter.keys().count()

# Create tuples for output in the format: ({tuple_of_item_ids_for_I}, item_id_for_j})
def out_format(three, four):
    for item in four:
        if item not in three:
            return (three, item)        
    else:
        None

# Check for interest of the fourth item being added to the tuple to be greater than equal to 0.02
interest = single_item_count.cartesian(confidence).filter(lambda x : x[0][0] in x[1][0][0]).map(lambda x : (x[1][0][1], x[1][0][0]) if ((x[1][1] - (float(x[0][1]) / float(total_no_of_users))) >= 0.02) else None).filter(lambda x : x != None).map(lambda x: out_format(x[0], x[1])).filter(lambda x : x != None).distinct().collect()

print('No. of Pair Items:', no_of_item_pairs))
print('No. of Triplet Items:', no_of_item_triplets)
print('No. of Quad Tuple Item:', len(interest))
print('Tuple of Four:', interest)

# Closing the spark context
sc.stop()