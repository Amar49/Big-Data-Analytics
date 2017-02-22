from pyspark import SparkContext
import re
import nltk
import ast
from operator import add
from nltk.util import ngrams
import numpy as np
import math
import random

new_data = txt_file.flatMap(lambda x: [line for line in x[1].split('\n')]).filter(lambda x : x != '').map(lambda x : ast.literal_eval(x.lower())).map(lambda x: (x[0], x[1][1])).collect()

map = {}
for i in range(0, 100):
	doc = random.choice(new_data)
	list = []
	if doc[1] in map:
		list = map[doc[1]]
		list.append(doc[0])
	else:
		list.append(doc[0])
		 
	map[doc[1]] = list	

print(map)	

training_data = txt_file.flatMap(lambda x: [line for line in x[1].split('\n')]).filter(lambda x : x != '').map(lambda x : ast.literal_eval(x.lower())).map(lambda x: (x[0], (x[1][0], x[1][1]))).filter(lambda x: x[1][1] not in map.keys() or x[0] not in map[x[1][1]])

testing_data = txt_file.flatMap(lambda x: [line for line in x[1].split('\n')]).filter(lambda x : x != '').map(lambda x : ast.literal_eval(x.lower())).map(lambda x: (x[0], (x[1][0], x[1][1]))).filter(lambda x: x[1][1] in map.keys() and x[0] in map[x[1][1]])

training_data.saveAsTextFile('Training')
testing_data.saveAsTextFile('Testing')


