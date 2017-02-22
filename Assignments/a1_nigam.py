############################################################
## Template Code for Big Data Analytics
## assignment 1, at Stony Brook Univeristy
## Fall 2016

## Aviral Nigam
## 110849584

## Python version used in this assignment is 3.5.2

## version 1.04
## revision history
##  .01 comments added to top of methods for clarity
##  .02 reorganized mapTask and reduceTask into "system" area
##  .03 updated reduceTask comment  from_reducer
##  .04 updated matrix multiply test
#############################################################

from abc import ABCMeta, abstractmethod
from multiprocessing import Process, Manager
from pprint import pprint
import numpy as np


# Python version used in this assignment is 3.5.2

##########################################################################
##########################################################################
# PART I. MapReduce

class MyMapReduce:#[TODO]
    __metaclass__ = ABCMeta

    def __init__(self, data, num_map_tasks=4, num_reduce_tasks=3): #[DONE]
        self.data = data  #the "file": list of all key value pairs
        self.num_map_tasks = num_map_tasks #how many processes to spawn as map tasks
        self.num_reduce_tasks = num_reduce_tasks # " " " as reduce tasks

    ###########################################################
    #programmer methods (to be overridden by inheriting class)

    @abstractmethod
    def map(self, k, v): #[DONE]
        print("Need to override map")


    @abstractmethod
    def reduce(self, k, vs): #[DONE]
        print("Need to override reduce")


    ###########################################################
    #System Code: What the map reduce backend handles

    def mapTask(self, data_chunk, map_to_reducer): #[DONE]
        #runs the mappers and assigns each k,v to a reduce task
        for (k, v) in data_chunk:
            #run mappers:
            mapped_kvs = self.map(k, v)
            #assign each kv pair to a reducer task
            for (k, v) in mapped_kvs:
                map_to_reducer.append((self.partitionFunction(k), (k, v)))

    # Calculating the ascii hash of the string
    def hash_string(self, s):
        ascii_val = 0
        for c in s:
            ascii_val += ord(c)
        return ascii_val

    def partitionFunction(self, k): #[TODO]
        #given a key returns the reduce task to send it

        node_number = self.hash_string(str(k)) % self.num_reduce_tasks

        return node_number


    def reduceTask(self, kvs, from_reducer): #[TODO]
        #sort all values for each key into a list
        #[TODO]

        key_dict = dict()
        for (k, vs) in kvs:
            if k in key_dict.keys():
                current_vs = key_dict[k]
                current_vs.append(vs)
                key_dict[k] = current_vs
            else:
                current_vs = []
                current_vs.append(vs)
                key_dict[k] = current_vs

        for key in key_dict:
            reduced_kvs = self.reduce(key, key_dict[key])
            (k, v) = reduced_kvs
            from_reducer.append((k, v))


        #call reducers on each key paired with a *list* of values
        #and append the result for each key to from_reducer
        #[TODO]


    def runSystem(self): #[TODO]
        #runs the full map-reduce system processes on mrObject

        #the following two lists are shared by all processes
        #in order to simulate the communication
        #[DONE]
        map_to_reducer = Manager().list() #stores the reducer task assignment and
                                          #each key-value pair returned from mappers
                                          #in the form: [(reduce_task_num, (k, v)), ...]
        from_reducer = Manager().list() #stores key-value pairs returned from reducers
                                        #in the form [(k, v), ...]

        #divide up the data into chunks accord to num_map_tasks, launch a new process
        #for each map task, passing the chunk of data to it.
        #hint: if chunk contains the chunk going to a given maptask then the following
        #      starts a process
        #      p = Process(target=self.mapTask, args=(chunk,map_to_reducer))
        #      p.start()
        #[TODO]
        list_of_tuples = self.data

        divide = int(len(list_of_tuples) / self.num_map_tasks)
        data_chunk = []
        processes = []

        count = 0
        for tuples in list_of_tuples:
            if count != divide:
                data_chunk.append(tuples)
                count += 1
            else:
                p = Process(target=self.mapTask, args=(data_chunk, map_to_reducer))
                p.start()
                processes.append(p)
                data_chunk = []

                data_chunk.append(tuples)
                count = 1

        if len(data_chunk) != 0:
            p = Process(target=self.mapTask, args=(data_chunk, map_to_reducer))
            p.start()
            processes.append(p)
            data_chunk = []

        #join map task processes back
        #[TODO]
        for p in processes:
            p.join()


        #print output from map tasks
        #[DONE]
        print("map_to_reducer after map tasks complete.")
        pprint(sorted(list(map_to_reducer)))

        #"send" each key-value pair to its assigned reducer by placing each
        #into a list of lists, where to_reduce_task[task_num] = [list of kv pairs]
        to_reduce_task = [[] for i in range(self.num_reduce_tasks)]
        for tasks in list(map_to_reducer):
            to_reduce_task[tasks[0]].append(tasks[1])

        #[TODO]

        #launch the reduce tasks as a new process for each.
        #[TODO]
        processes = []
        for reduce_task in to_reduce_task:
            p = Process(target=self.reduceTask, args=(reduce_task, from_reducer))
            p.start()
            processes.append(p)


        #join the reduce tasks back
        #[TODO]

        for p in processes:
            p.join()

        #print output from reducer tasks
        #[DONE]
        print("map_to_reducer after reducer tasks complete.")
        pprint(sorted(list(from_reducer)))

        #return all key-value pairs:
        #[DONE]
        return from_reducer


##########################################################################
##########################################################################
##Map Reducers:

class WordCountMR(MyMapReduce):  # [DONE]
    # the mapper and reducer for word count
    def map(self, k, v):  # [DONE]
        counts = dict()
        for w in v.split():
            w = w.lower()  # makes this case-insensitive
            try:  # try/except KeyError is just a faster way to check if w is in counts:
                counts[w] += 1
            except KeyError:
                counts[w] = 1
        return counts.items()

    def reduce(self, k, vs):  # [DONE]
        return (k, np.sum(vs))


class MatrixMultMR(MyMapReduce):  # [TODO]

    def dimension(self, m, n):
        self.row_m = m
        self.col_n = n

    def map(self, k, v):

        if k[0] == 'm':
            row_list = []
            for i in range(self.col_n):
                row_list.append(((k[1], i), (k[0], k[2], v)))
            return row_list
        else:
            row_list = []
            for i in range(self.row_m):
                row_list.append(((i, k[2]), (k[0], k[1], v)))
            return row_list

    def reduce(self, k, vs):

        matrix_m = {}
        matrix_n = {}
        for (matrix, key, value) in vs:
            if matrix == 'm':
                matrix_m[key] = value
            else:
                matrix_n[key] = value

        total = 0
        for key1, value1 in matrix_m.items():
            for key2, value2 in matrix_n.items():
                if key1 == key2:
                    total += value1 * value2

        return ((k[0], k[1]), total)


##########################################################################
##########################################################################
# PART II. Minhashing

# Finding the next prime nummber given an arbitary number
def find_next_prime(n):
    for p in range(n, 2*n):
        for i in range(2, p):
            if p % i == 0:
                break
        else:
            return p
    return -1

def minhash(documents, k=5): #[TODO]
    #returns a minhashed signature for each document
    #documents is a list of strings
    #k is an integer indicating the shingle size

    # num_of_documents represents the num of the column in the characteristic and signature matrix
    num_of_documents = len(documents)
    # num_of_hash_func represents the num of rows in the signature matrix
    # no of different hash functions
    num_of_hash_func = 100

    #Shingle Each Document into sets, using k size shingles
    #[TODO]
    shingles_map = {}
    shingles_set = set()

    for num_doc in range(0, num_of_documents):
        shingles = set()
        for char_index in range(0, len(documents[num_doc]) - k + 1):
            shingle = (documents[num_doc])[char_index:char_index + 5]
            shingles.add(shingle)
            shingles_set.add(shingle)

        shingles_map.setdefault(num_doc, []).append(list(shingles))

    shingles_list = list(shingles_set)

    characteristic_matrix = np.zeros((len(shingles_list), num_of_documents))

    for column in range(0, num_of_documents):
        shingles = shingles_map.get(column)
        row = 0
        for shingle in shingles_list:
            if shingle in str(shingles):
                characteristic_matrix[row][column] = 1
            row += 1

    #Perform efficient Minhash
    #[TODO]
    # Finding the next prime number for hash function
    if len(shingles_list) > 256:
        prime_num = find_next_prime(int(num_of_hash_func/(k-1)) * len(shingles_list))
    else:
        prime_num = find_next_prime(256 * num_of_hash_func)

    if prime_num == -1:
        prime_num = find_next_prime(num_of_hash_func * len(shingles_list) + 256 * num_of_hash_func)


    signatures = np.full((num_of_hash_func, num_of_documents), num_of_hash_func * prime_num, dtype=int)

    hash_val = []
    for num in range(0, num_of_hash_func):
        a = 3 * (num + 1)
        b = num + 1
        hash_val.append((a, b))

    for column in range(0, num_of_documents):

        for trial in range(0, num_of_hash_func):
            (a, b) = hash_val[trial]
            min_hash_val = signatures[trial][column]

            for row in range(0, len(shingles_list)):
                hash_row_value = (a * hash(shingles_list[row]) + b) % prime_num

                if characteristic_matrix[row][column] == 1:
                    if hash_row_value < min_hash_val:
                        min_hash_val = hash_row_value

            signatures[trial][column] = min_hash_val


    #Print signature matrix and return them
    #[DONE]
    pprint(signatures)
    return signatures #a minhash signature for each document


##########################################################################
##########################################################################

from scipy.sparse import coo_matrix


def matrixToCoordTuples(label, m):  # given a dense matrix, returns ((row, col), value), ...
    cm = coo_matrix(np.array(m))
    return zip(zip([label] * len(cm.row), cm.row, cm.col), cm.data)

def find_dimension(data):
    row_m = 0
    col_n = 0
    for (k1, v1) in data:
        if k1[0] == 'm':
            if row_m < k1[1]:
                row_m = k1[1]
        if k1[0] == 'n':
            if col_n < k1[2]:
                col_n = k1[2]

    row_m += 1
    col_n += 1

    return row_m, col_n

if __name__ == "__main__":  # [DONE: Uncomment peices to test]
    ###################
    ##run WordCount:
    data = [(1, "The horse raced past the barn fell"),
            (2, "The complex houses married and single soldiers and their families"),
            (3, "There is nothing either good or bad, but thinking makes it so"),
            (4, "I burn, I pine, I perish"),
            (5, "Come what come may, time and the hour runs through the roughest day"),
            (6, "Be a yardstick of quality."),
            (7, "A horse is the projection of peoples' dreams about themselves - strong, powerful, beautiful"),
            (8,
             "I believe that at the end of the century the use of words and general educated opinion will have altered so much that one will be able to speak of machines thinking without expecting to be contradicted.")]
    mrObject = WordCountMR(data, 4, 3)
    mrObject.runSystem()

####################
##run MatrixMultiply
# (uncomment when ready to test)
    data1 = list(matrixToCoordTuples('m', [[1, 2], [3, 4]])) + list(matrixToCoordTuples('n', [[1, 2], [3, 4]]))
    data2 = list(matrixToCoordTuples('m', [[1, 2, 3], [4, 5, 6]])) + list(matrixToCoordTuples('n', [[1, 2], [3, 4], [5, 6]]))
    data3 = list(matrixToCoordTuples('m', np.random.rand(20, 5))) + list(matrixToCoordTuples('n', np.random.rand(5, 40)))

    mrObject = MatrixMultMR(data1, 2, 2)
    (x, y) = find_dimension(data1)
    mrObject.dimension(x, y)
    mrObject.runSystem()
    mrObject = MatrixMultMR(data2, 2, 2)
    (x, y) = find_dimension(data2)
    mrObject.dimension(x, y)
    mrObject.runSystem()
    mrObject = MatrixMultMR(data3, 6, 6)
    (x, y) = find_dimension(data3)
    mrObject.dimension(x, y)
    mrObject.runSystem()

######################
## run minhashing:
# (uncomment when ready to test)
    documents = ["The horse raced past the barn fell. The complex houses married and single soldiers and their families",
                 "There is nothing either good or bad, but thinking makes it so. I burn, I pine, I perish. Come what come may, time and the hour runs through the roughest day",
                 "Be a yardstick of quality. A horse is the projection of peoples' dreams about themselves - strong, powerful, beautiful. I believe that at the end of the century the use of words and general educated opinion will have altered so much that one will be able to speak of machines thinking without expecting to be contradicted."]
    sigs = minhash(documents, 5)
