from pyspark import SparkContext
import re
import nltk
import ast
from operator import add
from nltk.util import ngrams
import numpy as np
import math
import sys

# Key = 1 - Unigram
# Key = 2 - Bigram
# Key = 3 - Trigram
key = sys.argv[3]

sc = SparkContext(appName = "Myapp")

txt_file = sc.wholeTextFiles(sys.argv[1])

#all tokens with idf count below this threshold will be removed
threshold = sys.argv[5]

no_of_doc = sys.argv[4]

# this dictionary the token as key and its unique id as value
global index_dict
index_dict = {}
# this dictionary stores the unique id as key and the token as value
global index_dict_rev
index_dict_rev = {}
#this dict stores the token as key and its idf count as value
global idf_count
idf_count = {}

#this function assigns the unique id to each token and populates it in the index_dict dictionary
def update_dict_uniq_words(u_words):
	data_set = set()
	for w in u_words:
		data_set.add(w[0])

	global total_unique_words
	total_unique_words = len(data_set)
	
	i = 0
	for w in data_set:
		index_dict[w] = i
		index_dict_rev[i] = w
		i += 1
		
#this function calculates the idf count of each token and populate it in the idf_count dictionary
def update_dict_tf_idf(words):
	for w in words:
		idf_count[w] = float(term_frequency_dict[w]) * math.log(float(no_of_doc)/documents_per_word_dict[w[0]])

#this function tokenizes the text into unigrams or bigrams or trigrams according to the key value		
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
    
# Get List of 
def unique(tokens):
    hset = set()
    for t in tokens:
        hset.add((t,1))
    return list(hset)
	
def unique(tokens, doc_id):
    hset = []
    for t in tokens:
        hset.append(((t, doc_id), 1))
    return hset
	
def mapper(tokens):
	final_token = set()
	for t in tokens:
		if t in index_dict:
			final_token.add(t)
			
	return list(final_token)
	
# this function calculates the idf vector for each document:  
# for each token in the document, store its idf value at the index which is equal to the id of the token
def vector(doc_id, tokens):
	vec = np.zeros((total_unique_words,), dtype=np.float)
	for t in tokens:
		if (t, doc_id) in idf_count:
			vec[index_dict[t]] = idf_count[(t, doc_id)]

	return vec

#
def vector_sum(author, vectors):
    """
    
    """
	final_vec = np.zeros((total_unique_words,), dtype=np.float)
	word_list = {}

	for i in range(0, total_unique_words):
		sum = 0
		for v in vectors:
			sum += float(v[i])

		final_vec[i] = sum
		if sum > 0:
			word_list[index_dict_rev[i]] = sum
	
	return author, word_list

#this function stores the result collected from the RDD and returns it in a dict
def map_convert(result_list):
	dict = {}
	for term in result_list:
		dict[term[0]] = term[1]

	return dict
	
#here we read the pre-processed data removed of all stopwords etc and then  we tokenize the text from the document and store the result as (docid, tokens, author name)	
tokenize_txt_data = txt_file.flatMap(lambda x: [line for line in x[1].split('\n')]).filter(lambda x : x != '').map(lambda x : ast.literal_eval(x.lower())).map(lambda x: (x[0], tokenize(x[1][0]), x[1][1]))

# here we store the total no of tokens per document 
total_words_per_doc = tokenize_txt_data.map(lambda x: (x[0], len(x[1]))).collect()

# we store the total tokens per document into a dictionary
total_words_per_doc_dict = map_convert(total_words_per_doc)

#we calculate the term frequency by counting the no. of words per document
term_frequency = tokenize_txt_data.flatMap(lambda x: unique(x[1], x[0])).reduceByKey(add).map(lambda x: (x[0], x[1])).filter(lambda x: x[1] > threshold)

#we calculate the number of documents per token
documents_per_word = term_frequency.map(lambda x: (x[0][0], [x[0][1]])).reduceByKey(add).map(lambda x: (x[0], len(x[1]))).collect()

#then we store above result in the dictionary
documents_per_word_dict = map_convert(documents_per_word)

#we store the term frequency result into a dict
term_frequency_dict = map_convert(term_frequency.collect())

update_dict_tf_idf(term_frequency_dict.keys())

update_dict_uniq_words(term_frequency_dict.keys())

filter_LFT = tokenize_txt_data.map(lambda x: (x[0], mapper(x[1]), x[2]))

vector_mapper = filter_LFT.map(lambda x: (x[0], vector(x[0], x[1]), x[2]))

#we calculate the naive bayes model for each document
naive_bayes = vector_mapper.map(lambda x: (x[2], [x[1]])).reduceByKey(add).map(lambda x: (x[0], x[1])).map(lambda x: (vector_sum(x[0], x[1]))).filter(lambda x: len(x[1]) > 0)

# Write naive bayes values to file
naive_bayes.saveAsTextFile(sys.argv[2])

# Write tf.idf values to file
idf_map = {}
for k in idf_count.keys():
	idf_map[k[0]] = idf_count[k]
	
f = open('idf', 'w+')
f.write(str(idf_map))
f.close

