import os
import json
import nltk
from bs4 import BeautifulSoup
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

#Sets of document_ids
mondegoSet = set()
infoSet = set()
irvineSet = set()

mondegoCounter = 0
infoCounter = 0
irvineCounter = 0

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

# This is the lemmatizer.
lemmatizer = WordNetLemmatizer()

# This is a dictionary of tokens.
tokenDict = {}


def computeWordFrequencies(aList, frequencyDict):
    for word in aList:
        if word in frequencyDict:
            frequencyDict[word] += 1
        else:
            frequencyDict[word] = 1
    # sortedDict = (sorted(frequencyDict.items(), key=lambda x: (-x[1], x[0])))
    return frequencyDict


if __name__ == "__main__":

    # Variables
    directory = "/Users/daniel/Downloads/WEBPAGES_CLEAN/"
    docId_url_dict = {}
    count = 0

    # Iterates through the directory first to fill up the dictionary to avoid data error
    for folder in os.listdir(directory):
        if folder.endswith(".json"):
            with open(directory + folder) as f:
                docId_url_dict = json.load(f)
            break

    # Iterates through folders
    for folder in os.listdir(directory):
        if folder.endswith(".tsv"):
            continue
        if folder.endswith(".json"):
            continue
        if folder.endswith(".DS_Store"):
            continue
        if folder == 'Output':
            continue

        #Iterates through files
        for file in os.listdir(directory + folder):
            count += 1
            docID = os.path.join(folder, file)
            # print("DocID: " + docID + " URL: " + docId_url_dict[docID])
            # Opens the file, ignores all non-english characters
            with open(directory + folder + "/" + file, 'r', encoding= 'ascii', errors= 'ignore') as f:
                mondegoCounter = 0
                irvineCounter = 0
                infoCounter = 0
                # Read the file
                contents = f.read()
                # Make a BS4 object from the file so we can parse tags like <strong></strong>, <h1></h1>
                soup = BeautifulSoup(contents, 'lxml')
                # Code below is suppose to extract the tag AND the content between it and store it in extractedTokens
                # For example, if your file contains <h1> <strong> Hi my name is Daniel </strong> </h1>
                # extractedTokens will return [<strong> Hi my name is Daniel </strong>] and also remove all of that from soup.text
                # We will need to find a way to split that 1 list element into multiple i.e. [<strong>, Hi, my, name, is, Daniel, </strong>]
                # Then we need to store the token in the database, update its frequency and score it
                # extractedTokens = [s.extract() for s in soup('strong')]
                # Need to check if each character is alphanumeric, ie we do not need to tokenize characters such as @, !, #, $
                alphaNumString = ''
                overallWords = 0
                for word in soup.text:
                    if word.isalnum():
                        alphaNumString += word
                    else:
                        overallWords += 1
                        if (alphaNumString != ''):
                            word_tokens = word_tokenize(alphaNumString)
                            # print(word_tokens)
                            tokenList = [w for w in word_tokens if not w in stopWord]

                            # only lemmatizes nouns by default...so we are only lemmatizing only part of the tokens
                            for w in tokenList:
                                w = lemmatizer.lemmatize(w)
                                if w == "Mondego":
                                    mondegoCounter += 1
                                    mondegoSet.add(docID)
                                    # print("Mondego: " + str(mondegoSet))
                                    # print ("Mondego DocID:", docID," Freq:", mondegoCounter)
                                    f = open("/Users/daniel/Downloads/WEBPAGES_CLEAN/Output/Mondego.txt", "a")
                                    temp = "DocID: {} Freq: {} URL: {} tf_id: {}\n".format(docID, mondegoCounter, docId_url_dict[docID], (mondegoCounter * 2)/overallWords)
                                    f.write(temp)
                                    f.close()

                                if w == "Informatics":
                                    infoCounter += 1
                                    infoSet.add(docID)
                                    # print("Informatics: " +str(infoSet))
                                    # print("Informatics DocID::",docID, " Freq:", infoCounter)
                                    f = open("/Users/daniel/Downloads/WEBPAGES_CLEAN/Output/Infomatics.txt", "a")
                                    temp = "DocID: {} Freq: {} URL: {} tf_id: {}\n".format(docID, infoCounter, docId_url_dict[docID], (infoCounter * 2)/overallWords)
                                    f.write(temp)
                                    f.close()

                                if w == "Irvine":
                                    irvineCounter += 1
                                    irvineSet.add(docID)
                                    # print("Irvine: " + str(irvineSet))
                                    # print("Irvine DocID:", docID, " Freq:", irvineCounter)
                                    f = open("/Users/daniel/Downloads/WEBPAGES_CLEAN/Output/Irvine.txt", "a")
                                    temp = "DocID: {} Freq: {} URL: {} tf_id: {}\n".format(docID, irvineCounter, docId_url_dict[docID], (irvineCounter * 2)/overallWords)
                                    f.write(temp)
                                    f.close()

                            #computeWordFrequencies(tokenList, tokenDict)
                            alphaNumString = ''
                # print(tokenDict)
    print("Count: " + str(count))
