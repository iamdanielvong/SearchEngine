import os
import json

if __name__ == "__main__":

    directory = "/Users/daniel/Downloads/WEBPAGES_CLEAN/"
    docId_url_dict = {}

    for folder in os.listdir(directory):
        if folder.endswith(".json"):
            with open(directory + folder) as f:
                docId_url_dict = json.load(f)
            break

    for folder in os.listdir(directory):
        if folder.endswith(".tsv"):
            continue
        if folder.endswith(".json"):
            continue

        for file in os.listdir(directory + folder):
            docID = os.path.join(folder, file)
            print("DocID: " + docID + " URL: " + docId_url_dict[docID])

