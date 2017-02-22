###########################################
# Author: Aviral Nigam
# SBU ID: 110849584
###########################################

from pyspark import SparkContext
from operator import add
import math
import sys

# For finding tuple of items and users and returning a list.
def find_tuple(keys, item, list):
    result = []
    for key in keys:
        flag = True
        for k in list:
            if key == k[0]:
                result.append((k[0], item, k[1]))
                flag = False
                break
    
    return result

# For getting normalization factor of a list.    
def normalization_factor(list):
    count = 0
    sum = 0.0
    for k in list:
        count += 1
        sum += float(k[1])
    
    norm_factor = float(sum) / float(count)
    
    return norm_factor

# For normalizing rating per item and return normalized list.   
def normalization(list):
    count = 0
    sum = 0.0
    for k in list:
        count += 1
        sum += float(k[1])
    
    norm_factor = float(sum) / float(count)
    
    new_list = []
    for k in list:
        new_list.append((k[0], float(k[1]) - norm_factor))
            
    return new_list

# For finding cosine between user, normalized rating lists.
def cosine(list1, list2):
    sum = 0.0;
    for k1 in list1:
        for k2 in list2:
            if k1[0] == k2[0]:
                sum += (float(k1[1]) * float(k2[1]))
    
    sum_list1 = 0.0
    sum_list2 = 0.0
    
    for k in list1:
        sum_list1 += (k[1] * k[1])

    for k in list2:
        sum_list2 += (k[1] * k[1])
            
    if sum == 0.0 or sum_list1 == 0.0 or sum_list2 == 0.0:
        return -10000
        
    cosine = sum / (math.sqrt(sum_list1) * math.sqrt(sum_list2))
    
    return cosine
 
# For finding similarity weighted average of normalized ratings  
def user_avg(list):
    count = 0.0;
    sum = 0.0;
    for k in list:
        if k[2] >=0:
            count += float(k[2])
            sum += (float(k[1]) * float(k[2]))
    
    if count == 0.0:
        return 0
        
    avg = float(sum) / float(count)

    return avg


filename = str(sys.argv[1])
item = str(sys.argv[2])
    
# Creating Spark Context
sc = SparkContext(appName="Recommendation System")

# Loading data from cluster and sort by timestamp to get only one latest rating per item and user.
csv_data = sc.textFile(filename).map(lambda line: line.split(",")).filter(lambda line: len(line) > 3).map(lambda line: ((line[0], line[1]), (line[2], line[3]))).groupByKey().map(lambda line: (line[0], sorted(list(line[1]), key=lambda tup: tup[1]))).map(lambda line: (line[0][0], line[0][1], line[1][0][0]))

# Filter to items associated with at least 25 users
items_data = csv_data.map(lambda s: (s[1], [(s[0], s[2])])).reduceByKey(add).map(lambda s: (s[0], s[1])).filter(lambda s: len(s[1]) >= 25)
  
# Filter to users associated with at least 10 items  
user_data = items_data.flatMap(lambda d: [(x[0], d[0], x[1]) for x in d[1]]).map(lambda x: (x[0], [(x[1], x[2])])).reduceByKey(add).map(lambda s: (s[0], s[1])).filter(lambda x: len(x[1]) >= 10)

# Get the list of all the users
list_of_users = user_data.keys().collect()

# Get the list of all users who have rated some items and get item user rating tuple. 
cartesian_data_1 = user_data.flatMap(lambda d: [(d[0], x[0], x[1]) for x in d[1]]).map(lambda x: (x[1], [(x[0], x[2])])).reduceByKey(add).map(lambda x: (x[0], x[1]))

# Create item user rating tuples to get matrix form.
final_data = cartesian_data_1.flatMap(lambda d: find_tuple(list_of_users, d[0], d[1])).map(lambda x: (x[0], x[1], x[2])).distinct().map(lambda x: (x[1], [(x[0], x[2])])).reduceByKey(add).map(lambda x: (x[0], x[1]))

# Get normalization factor of target item.
target_item_factor = final_data.filter(lambda x: x[0] in item).map(lambda x: (normalization_factor(x[1]))).collect()

# Normalize the ratings for each item.
normalized_data = final_data.map(lambda x: (x[0], normalization(x[1])))

# Extract target item.
with_item = normalized_data.filter(lambda x: x[0] in item)

# All data except the target item.
without_item = normalized_data.filter(lambda x: x[0] != item)

# Calculate cosine similarity between normalized ratings of target item and other items and take nearest 50 neighbours
cosine_data = with_item.cartesian(without_item).map(lambda x: (x[0][0], [(x[1][0], x[1][1], cosine(x[0][1], x[1][1]))])).reduceByKey(add).map(lambda s: (s[0], sorted(s[1], key=lambda tup: tup[2], reverse=True))).map(lambda x: x[1][:50])

# Get the final averaged predicted rating per item.
final_user_data = cosine_data.flatMap(lambda d: [(x[0], x[1], x[2]) for x in d]).flatMap(lambda d: [(x[0], d[0], x[1], d[2]) for x in d[1]]).map(lambda x: (x[0], [(x[1], x[2], x[3])])).reduceByKey(add).map(lambda x: (x[0], user_avg(x[1]))).collect()

# Get the target item to produce result.
with_item_collect = with_item.collect()

# Get list of all users and add ratings to only those rated previosuly. Others get zero.
final_list = []

for u in list_of_users:
    final_list.append((u, 0))

for u in with_item_collect[0][1]:
    if u[0] in list_of_users:
        final_list.remove((u[0], 0))
        final_list.append((u[0], u[1])) 

# De-normalize the rating for the item based on average rating calculated.
result = []
avg = float(target_item_factor[0])
for user in final_list:
    flag = True
    if user[1] == 0:
        for u in final_user_data:
            if user[0] == u[0] and u[1] != 0:
                result.append((u[0], u[1] + avg))
                flag = False
                break
                
    if flag == True:
        result.append((user[0], user[1] + avg))
    
final_result = (item, result)

print(final_result)

# Closing the spark context
sc.stop()

