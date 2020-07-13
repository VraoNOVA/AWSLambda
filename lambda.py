import json
import ast
import time
from time import gmtime, strftime

def lambda_handler(event, context):
    print("In lambda handler")
    print(event)
    print (type(event))
    print(event)
    millis = int(round(time.time() * 1000))
    print (str(millis))
    print("\n")
    print(str(event["body"]))
    key1 = "insertTime"
    currentD =  (ast.literal_eval(str(event["body"])))
    currentD[key1] = str(millis)
    resp = {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": "*",
        },
        "body": "\n"+(str(currentD))
        
    }

    return resp
