import re
import nltk
import ast
from operator import add
from nltk.util import ngrams
import numpy as np
import math
import codecs

word_id_map = {}
idf_map = {}
author_id_map = {}
model_map = {}
key = 1

def parse_files_from_map_reduce(doc_author_list, dictionary, model):
	dictionary_file = open(dictionary)
	index = 0
	for line in dictionary_file.readlines():
		splits = line.split("\t")
		word_id_map[splits[0]]= index
		idf_map[splits[0]] = int(splits[1])
		index += 1

	authors_file = open(doc_author_list)
	index = 0
	for line in authors_file.readlines():
		splits = line.split("\t")
		if splits[1] not in author_id_map:
			author_id_map[splits[1]] = index
			index += 1

	model_file = open(model)
	index = 0
	for line in model_file.readlines():
		splits = line.split("\t")
		author_id = splits[0].substring(1, len(splits[0]) - 1)
		vector_vals = splits[1].substring(1, len(splits[1]) - 1)
		vals = vector_vals.split(",")
		per_author_vals = []
		for val in vals:
			index_val = val.split(":")
			per_author_vals[int(index_val[0])] = float(index_val[1])
		model_map[author_id] = per_author_vals
		
def parse_files_from_spark(data):
	read_data = []
	for line in data.readlines():
		read_data.append(ast.literal_eval(line.lower()))
	
	return read_data
	
def tokenize(text):
	tokens = nltk.word_tokenize(str(text))
	if key == 2:
		bigrams = ngrams(tokens, 2)
		tokens = []
		for i in bigrams:
			tokens.append(i)
	elif key == 3:
		trigrams = ngrams(tokens, 3)
		tokens = []
		for i in trigrams:
			tokens.append(i)

	return tokens
	
def word_count(tokens):
    wordcount = {}
    for word in tokens:
        if word not in wordcount:
            wordcount[word] = 1
        else:
            wordcount[word] += 1

    return wordcount

f = codecs.open('file3', 'r', encoding='utf8')
file_data = parse_files_from_spark(f)
f.close()

f = codecs.open('part-00000', 'r', encoding='utf8')
model_data = parse_files_from_spark(f)
f.close()

f = codecs.open('part-00001', 'r', encoding='utf8')
model_data1 = parse_files_from_spark(f)
f.close()

model_data = model_data + model_data1

f = open('idf2', 'r+')
idf = parse_files_from_spark(f)
f.close()
idf = idf[0]

count = 0
correct = 0
author_hashset = set()
for f in file_data:
    count += 1
    idf_val = {}
    tokens = tokenize(f[1][0])
    term_freq = word_count(tokens)
    index_dict = {}
    i = 0
    for term in term_freq.keys():
        index_dict[term] = i
        i += 1

    term_vec = np.zeros((len(term_freq.keys()),), dtype=np.float)
    sum_vec = 0.0
    for term in term_freq.keys():
        if term in idf:
            term_vec[index_dict[term]] = float(term_freq[term]) * float(idf[term])
            sum_vec += float(term_freq[term]) * float(idf[term])

    for term in term_freq.keys():
        if term in idf:
            term_vec[index_dict[term]] = term_vec[index_dict[term]] / sum_vec

    author_pred = []
    max = 0.0
    for m in model_data:
        author_vec = np.zeros((len(term_freq.keys()),), dtype=np.float)
        sum_vec = 0.0
        for k in m[1].keys():
            if k in index_dict:
                author_vec[index_dict[k]] = float(m[1][k])
                sum_vec += float(m[1][k])

        for k in m[1].keys():
            if k in index_dict:
                author_vec[index_dict[k]] = float(m[1][k])/sum_vec

        sum = 0.0
        for i in range(0, len(term_freq.keys())):
            product = author_vec[i] * term_vec[i]
            if product > 0.0:
                sum += math.log(product)

        author_pred.append((m[0], sum))
        author_pred = sorted(author_pred, key=lambda tup: tup[1], reverse = True)

    author_hashset.add(f[1][1])
	if author_pred[0] == f[1][1]:
		print(author_pred[0])
		print(f[1][1])

		print('###############################################################')
		correct += 1

print("Total Files: " + str(count + 1))
print("Total Correct Predict: " + str(correct + 1))
print("Total Unique Authors: " + str(len(author_hashset)))
