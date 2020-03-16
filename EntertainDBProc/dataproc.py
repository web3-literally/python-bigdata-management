# -*- coding: utf-8 -*-
"""
Created on Thu Sep 12 06:32:52 2019

@author: Lyu CSTAR
"""
import gzip
import csv
import os
import sys
import threading
import imdb

INPUT_DIR = 'Input Files/'
OUTPUT_FILE = 'Output Files/Entertainment DB.csv'
imdb_data = dict()
imdb_id_arr = []
threads = []
threadLock = threading.Lock()

# set csv enable to read huge amount of data
maxInt = sys.maxsize
while True:
    # decrease the maxInt value by factor 10
    # as long as the OverflowError occurs.
    try:
        csv.field_size_limit(maxInt)
        break
    except OverflowError:
        maxInt = int(maxInt / 10)

# read entertainment db
def readEntertainDB(filename):
    try:
        with open(filename, mode='rt', encoding='UTF8') as file:
            datareader = csv.reader(file)
            # row_count = sum(1 for row in datareader)
            # print('row count of file: ' + filename + ' is ' + str(row_count))
            row = next(datareader)  # yield the header row
            for row in datareader:
                imdb_id = ''
                type = ''
                title = ''
                genre = ''
                if (len(row) > 0):
                    imdb_id = row[0]
                if (len(row) > 1):
                    type = row[1]
                if (len(row) > 2):
                    title = row[2]
                if (len(row) > 3):
                    genre = row[3]

                if (not imdb_id in imdb_data):      # imdb id already exist
                    imdb_item = imdb.Imdb()
                    imdb_item.id = imdb_id
                    imdb_item.type = type
                    imdb_item.title = title
                    imdb_item.genre = genre
                    imdb_data[imdb_id] = imdb_item
    except Exception as e:
        print('exception while reading entertainment db : ' + str(e))

# read production co
def readProductionco(filename):
    try:
        with open(filename, mode='rt', encoding='UTF8') as file:
            datareader = csv.reader(file)
            # row_count = sum(1 for row in datareader)
            # print('row count of file: ' + filename + ' is ' + str(row_count))
            row = next(datareader)  # yield the header row
            for row in datareader:
                imdb_id = ''
                production_co = ''
                if (len(row) > 0):
                    imdb_id = row[0]
                if (len(row) > 1):
                    production_co = row[1]

                if (imdb_id in imdb_data):      # imdb id already exist
                    imdb_item = imdb_data[imdb_id]
                    imdb_item.production_co = production_co
    except Exception as e:
        print('exception when read title : ' + str(e))

# read file and make output
def thread_function(inputname, datawriter):
    print('start thread: with ' + inputname)
    try:
        with gzip.open(inputname, 'rt', encoding='UTF8') as readFile:
            datareader = csv.reader(readFile)
            row = next(datareader)  # yield the header row
            for row in datareader:
                if len(row) == 0:
                    continue
                imdb_id = row[len(row) - 1]         # consider last item is imdb id
                threadLock.acquire()                # get lock to synchronize threads
                if (not imdb_id in imdb_id_arr):    # not write this imdb id yet
                    imdb_id_arr.append(imdb_id)     # mark this imdb id as processed
                    threadLock.release()            # free lock to release next thread
                    if (imdb_id in imdb_data):      # imdb id exist, write to output file
                        imdb_item = imdb_data[imdb_id]
                        row = [imdb_item.id, imdb_item.type, imdb_item.title, imdb_item.genre,
                               imdb_item.production_co]
                        datawriter.writerow(row)
                        print(row)
                else:
                    threadLock.release()            # free lock to release next thread

        print('thread is exiting (with ' + inputname + ')')
    except Exception as e:
        print('exception in thread (with ' + inputname + ') error: ' + str(e))

# read entertainment db
print('start reading entertainment db file')
readEntertainDB('Database files/Entertainment DB.csv')
print('end reading entertainment db file, record count is ' + str(len(imdb_data)))

# read production co
print('start reading production co')
readProductionco('Database files/Reference sheet for PRODUCTION CO.csv')
print('production co column has added to entertainment db')

# start threading section
# get file list in 'Input Files' directory, and make one thread for each file
try:
    with open(OUTPUT_FILE, 'wt', encoding='UTF8') as writeFile:
        datawriter = csv.writer(writeFile)
        for r, d, f in os.walk(INPUT_DIR):
            for file in f:
                thread = threading.Thread(target=thread_function, args=(INPUT_DIR + file, datawriter))
                thread.start()
                threads.append(thread)

        # wait for all threads to complete
        for thread in threads:
            thread.join()
except Exception as e:
    print('exception in main thread : ' + str(e))

print('Processing is finished')
