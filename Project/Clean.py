from pyspark import SparkContext
from nltk.corpus import stopwords
import xml.etree.ElementTree as ET
import tarfile
import string


def is_ascii(s):
    return all(ord(c) < 128 for c in s)
	
def no_stop_words(text):
	text = str(text.encode("utf-8"))
	exclude = set(string.punctuation)
	text = ''.join(ch for ch in text if ch not in exclude)
	text = ' '.join([word for word in text.split() if word not in cachedStopWords and word.isalnum and len(word) > 2])
	return text.lower()

def parse_metadata(data):
	try:
		root = ET.fromstring(data)
		document_id = root.items()[0][1]
		list_authors = []
		for i in root.iter('authors'):
			for j in i.iter('author'):
				list_authors.append(j.find('name').text.lower())

		if len(list_authors) < 1:
			return None
		
		return document_id, list_authors[0].encode("utf-8")
	except:
		return None

def read_data(filename, file):
	if filename in author_dict:
		return author_dict[filename], file
	else:
		return None

		
cachedStopWords = stopwords.words("english")

sc = SparkContext(appName = "Myapp")

file_path = 'C:\\Users\\Aviral\\Documents\\Data\\txt\\*.txt'

txt_file = sc.wholeTextFiles(file_path)

# RDD Containing data mapping like (document_id, paper content excluding stop words)
# e.g. - [('10.1.1.83.9484', "About paper medical sceince")]
txt_data = txt_file.map(lambda x : (x[0].split('/')[-1].replace('.txt', ''), no_stop_words(x[1])))

metadata_file = sc.wholeTextFiles('C:\\Users\\Aviral\\Documents\\Data\\xml\\*.xml')

# RDD Containing data mapping like (document_id, list(Authors))
xml_meta_data = metadata_file.map(lambda x: parse_metadata(x[1]))

xml_meta_data = xml_meta_data.filter(lambda x: x is not None)#.collect()

#author_dict = dict(xml_meta_data)
join_data = txt_data.join(xml_meta_data)

join_data.saveAsTextFile('datatotal')
