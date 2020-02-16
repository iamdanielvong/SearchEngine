import os

if __name__ == "__main__":
    print("hello worlds")
    directory = "/Users/daniel/Downloads/WEBPAGES_CLEAN"
    for folder in os.listdir(directory):
        #print(folder)
        if folder.endswith(".json") or folder.endswith(".tsv"):
            print("bookkeeping files")
            continue
        for file in os.listdir("/Users/daniel/Downloads/WEBPAGES_CLEAN/" + folder):
            print(os.path.join(folder, file))
