# Python program to illustrate the concept                                                                                                                                                                                                   
# of threading                                                                                                                                                                                                                               
import threading
import os
import requests
import json
import csv
import ast

def printTask (obj):
    r = requests.post('https://vizq9x2auh.execute-api.us-east-2.amazonaws.com/default/NewLambda', data = obj)
    x =  r.text
    y =  (ast.literal_eval(str(x)))
    key1 = 'MONTH'
    key2 = 'FLIGHT_NUMBER'
    key3 = 'insertTime'
    flightNumb = y[key2]
    month = y[key1]
    insertTime = y[key3]
    print("FlightNumb:  "  + flightNumb + "   Month:    "+month+"   insertTime:   "+insertTime)
    print("\n")

if __name__ == "__main__":
    csvfile = open('helix2.csv', 'r')
    jsonfile = open('vivek.json', 'w')
    reader = csv.DictReader(csvfile)
    counter = 0
    limit = 32
    threads = []
    for row in reader:
        counter += 1
        print (str(row))
        print ("\n*************************\n")
        #constructing a json object to send to api , so it will be processed by lambda                                                                                                                                                       
        value = row
        value = json.dumps(value)
        value2 = json.loads(value)
        payLoad = str(value2)
        tx = threading.Thread(target=printTask, args=(payLoad,))
        tx.start()
        threads.append(tx)
        #writing stuff to json file for error and debugging                                                                                                                                                                                  
        json.dump(row, jsonfile)
        jsonfile.write('\n')
        if (counter == limit):
            break
    for threadObj in threads:
        threadObj.join()
    print("\n\n\n\n\n\n\n\n")
    print("Done!")
