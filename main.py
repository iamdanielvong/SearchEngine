import os
import json
import nltk
from bs4 import BeautifulSoup
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk import pos_tag
import re
from collections import Counter
import math
import psycopg2
import operator

stopWord = ["a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren't",
            "as", "at", "be", "because", "been",
            "before", "being", "below", "between", "both", "but", "by", "can't", "cannot", "could", "couldn't",
            "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during", "each", "few", "for",
            "from", "further", "had", "hadn't", "has", "hasn't",
            "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers",
            "herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll",
            "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", "let's",
            "me", "more", "most", "mustn't", "my",
            "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought", "our",
            "ours", "ourselves", "out",
            "over", "own", "same", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't",
            "so", "some", "such", "than", "that", "that's", "the", "their",
            "theirs", "them", "themselves", "then", "there",
            "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "through", "to",
            "too",
            "under", "until", "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were",
            "weren't", "what", "what's", "when", "when's", "where", "where's", "which",
            "while", "who", "who's", "whom", "why", "why's", "with", "won't", "would", "wouldn't", "you",
            "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves"]

stopWord = set(stopWord)


try:
    connection = psycopg2.connect(user = "postgres",
                                  password = "mysecretpassword",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "postgres")

    cursor = connection.cursor()
    # Print PostgreSQL Connection properties
    #print ( connection.get_dsn_parameters(),"\n")

    # Print PostgreSQL version
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print("You are connected to - ", record,"\n")

except (Exception, psycopg2.Error) as error :
    print ("Error while connecting to PostgreSQL", error)
# finally:
#     #closing database connection.
#         if(connection):
#             cursor.close()
#             connection.close()
#             print("PostgreSQL connection is closed")



def computeWordFrequencies(aList, frequencyDict):
    for word in aList:
        if word in frequencyDict:
            frequencyDict[word] += 1
        else:
            frequencyDict[word] = 1
    # sortedDict = (sorted(frequencyDict.items(), key=lambda x: (-x[1], x[0])))
    return frequencyDict


def insert_row(doc_id, word, url, frequency, tf):  # , idf, tf_idf):
    sql = """INSERT INTO search_engine(doc_id, word, url, frequency, tf) 
    VALUES(%s, %s, %s, %s, %s) 
    ON CONFLICT DO NOTHING;"""
    try:
        connection = psycopg2.connect(user="postgres",
                                      password="mysecretpassword",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="postgres")

        cursor = connection.cursor()
        cursor.execute(sql, (doc_id, word, url, frequency, tf))  # , idf, tf_idf))
        connection.commit()
        cursor.close

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if connection is not None:
            connection.close()


def compute_tf(tf_dict, freq_dict, total_term):
    for word in tf_dict:
        tf_dict[word] = freq_dict[word] / total_term
        


def search_engine():
    directory = "/Users/daniel/Downloads/WEBPAGES_RAW"
    lemmatizer = WordNetLemmatizer()
    tokenDict = {}
    docId_url_dict = {}
    count = 0
    overallWords = 0
    freqDictPerDocument = {}
    tfDictPerDocument = {}

    with open(directory + '/bookkeeping.json') as f:
        docId_url_dict = json.load(f)

    for docID in docId_url_dict:

        # for file in os.listdir(directory + '/' + folder):
        #     docID = os.path.join(folder, file)  # key in bookkeeping.json file

        # Opens the file, ignores all non-english characters
        with open(directory + '/' + docID, 'r', encoding='ascii', errors='ignore') as f:
            contents = f.read()
            soup = BeautifulSoup(contents, 'lxml')
            alphaNumString = ''
            tokens = re.split('[^a-zA-Z]', soup.text.lower())

            # Removes empty string from the list after calling re.split
            tokens = list(filter(None, tokens))

            # Removes any instances of stop words from the token list
            tokens = [w for w in tokens if not w in stopWord]

            # Gets the word type using nltk (noun, verb, etc) and lemmatizes the word based on its type
            indexI = 0;
            for word, tag in pos_tag(tokens):
                wntag = tag[0].lower()
                wntag = wntag if wntag in set(['a', 'r', 'n', 'v']) else None
                if wntag:
                    tokens[indexI] = lemmatizer.lemmatize(word, wntag)

                indexI += 1

            freqDictPerDocument = (Counter(tokens))

            # Get the total number of words in current document
            totalTermsOfDocument = sum(freqDictPerDocument.values())

            # Create a new dict for tf calculation and copy the keys to reuse
            tfDictPerDocument = dict.fromkeys(freqDictPerDocument)

            for word in tfDictPerDocument:
                tfDictPerDocument[word] = freqDictPerDocument[word] / totalTermsOfDocument
                insert_row(docID, word, docId_url_dict[docID], freqDictPerDocument[word], tfDictPerDocument[word])

            # Create a new dict for idf calculation and copy the keys to reuse
            idfDictPerDocument = dict.fromkeys(freqDictPerDocument)

            # Create a new dict for FINAL tf-idf calculation and copy the keys to reuse
            tfidfDictPerDocument = dict.fromkeys(freqDictPerDocument)

        overallWords = overallWords + len(tokens)






def rootSumSquare(queries , scoreType, doc_id):
    total = 0
    for key,val in enumerate(queries):
        sql = """SELECT {}  
        FROM search_engine 
        WHERE word = %s AND doc_id = %s ORDER BY tf_idf DESC LIMIT 50""".format(scoreType)
        cursor.execute(sql, (val, doc_id))
        results = cursor.fetchall()
        for row in results:

            total += row[0] ** 2

    return math.sqrt(total)


def dotProduct(queries, doc_id):
    total = 0
    for key, val in enumerate(queries):
        sql = """SELECT tf_idf, idf
            FROM search_engine 
            WHERE word = %s AND doc_id = %s ORDER BY tf_idf DESC LIMIT 50"""
        cursor.execute(sql, (val, doc_id))

        results = cursor.fetchall()
        for row in results:
            if row[1] >= 1:
                total += (row[0] * (row[1] * .1))
            else:
                total += row[0] * row[1]

    return total


def cosineSimilarity(queries, docList):
    cosineDict = {}
    for key,val in enumerate(docList):
        score = dotProduct(queries, val) / (rootSumSquare(queries, 'idf', val) + rootSumSquare(queries, 'tf', val))
        cosineDict[val] = score
    return cosineDict


def UserInput():
    directory = "/Users/daniel/Downloads/WEBPAGES_RAW"
    with open(directory + '/bookkeeping.json') as f:
        docId_url_dict = json.load(f)

    try:
        connection = psycopg2.connect(user="postgres",
                                      password="mysecretpassword",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="postgres")

        cursor = connection.cursor()
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)

    while(1):

        query = input("What do you want to search\n> ")
        queryList = query.split()

        if (queryList[0] == 'quit'):
            cursor.close
            connection.close
            break

        if len(queryList) == 1:
            sql = """SELECT url FROM search_engine WHERE word=%s ORDER BY tf_idf DESC LIMIT 20"""
            cursor.execute(sql, [queryList[0]])
            results = cursor.fetchall()
            counter = 1
            for row in results:
                print("{}: {}".format(counter, row[0]))
                counter += 1
                if counter == 21:
                    break

        else:
            # Initialize the overall set
            sql = "SELECT doc_id FROM search_engine WHERE word = %s ORDER BY tf_idf DESC LIMIT 500"
            cursor.execute(sql, [queryList[0]])
            results = cursor.fetchall()
            docid_list = []
            for row in results:
                docid_list.append(row[0])
            overall_ID_set = set(docid_list)

            for key, value in enumerate(queryList):
                sql = "SELECT doc_id FROM search_engine WHERE word = %s ORDER BY tf_idf DESC LIMIT 500"
                cursor.execute(sql, [value])
                results = cursor.fetchall()
                temp_list = []
                for row in results:
                    temp_list.append(row[0])

                temp_ID_set = set(temp_list)
                overall_ID_set = set(list(overall_ID_set & temp_ID_set))

            resultDict = cosineSimilarity(queryList,list(overall_ID_set))
            result_sorted_keys = sorted(resultDict.items(), key=operator.itemgetter(1), reverse=True)
            #print(result_sorted_keys)
            counter = 1
            for r in result_sorted_keys:
                print("{}: {}".format(counter, docId_url_dict[r[0]]))
                counter += 1
                if counter == 21:
                    break


if __name__ == "__main__":
    UserInput()
