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
    print ( connection.get_dsn_parameters(),"\n")

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

    # Iterates through the directory first to fill up the dictionary to avoid data error
    # for folder in os.listdir(directory):
    #     if folder.endswith(".json"):
    #         with open(directory + "/" + folder) as f:
    #             docId_url_dict = json.load(f)
    #         break

    # Iterates through folders
    # for folder in os.listdir(directory):
    #     if folder.endswith(".tsv") or folder.endswith(".json") or folder.endswith(".DS_Store") or folder == 'Output':
    #         continue

    # Iterates through files

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

            # insert the docID | word | word freq | URL
            # not sure if code below will work


            # Replace 9999999999 with # of documents with term t in it which is retrieved using SQL (10 argument is log base)
            # idfDictPerDocument[word] = math.log(37497 / 999999999, 10)

            # Now you want to calculate the tf-idf score for each word in the same loop
            # tfidfDictPerDocument[word] = tfDictPerDocument[word] * idfDictPerDocument[word]

            # Now you want to update the rows to add the tf-idf score of that word

        overallWords = overallWords + len(tokens)

    # Prints the total # of words in ALL documents combined
    # print(overallWords)



'''
sql statement should be something like 
select tf,idf, docId 
from table
where word = data[i]
order by tfidf
limit by a reasonable amount 20 - 100


def rootSumSquare(queries , scoreType, doc_id):
    # sql statement should look like this
    select tf/idf 
    from table
    where word = queries[i]
    and doc_id = doc_id
    # this should go inside the for loop

    total = 0
    for len of queries:
        total += (tf or idf)^2

    return square root of total


def dotProduct(queries, doc_id):
    # sql statement should look like this
    select tfidf, idf
    from table 
    where docid = doc_id
    and word = queries[i]
    # this should go inside the for loop

    total = 0
    for len of queries:
        total += tfidf * idf

    return total

def cosineSimilarity(queries, doc_id): 
    score = dotProduct(data, doc_id) / ( rootSumSquare(queries, idf, doc_id) + rootSumSquare(queries, tf, doc_id))
    return score
'''

def UserInput():
    try:
        connection = psycopg2.connect(user="postgres",
                                      password="mysecretpassword",
                                      host="127.0.0.1",
                                      port="5432",
                                      database="postgres")

        cursor = connection.cursor()
        print("successfully connected")
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
        else:
            # get a list of docs to look through 
            # sql statement could be something like 
            # join all the docIDs that have the words in query 
            # limit by tfid score and limit search to like 50-100
            # put all the results in a docList
            '''
            for i in docList:
               dict{docList[1]} =  cosineSimilarity(query, docList[i])
            '''
            # print top 20 results from dict
        for key,val in enumerate(results):
            print(val)


if __name__ == "__main__":
    UserInput()
    print('Finished!!!!')
