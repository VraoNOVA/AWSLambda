import json

import ast

import time
from time import gmtime, strftime

import pymysql

from DBUtils.PooledDB import PooledDB

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
    YEAR = currentD['YEAR']
    MONTH = currentD['MONTH']
    DAY = currentD['DAY']
    DAY_OF_WEEK = currentD['DAY_OF_WEEK']
    AIRLINE = currentD['AIRLINE']
    FLIGHT_NUMBER = currentD['FLIGHT_NUMBER']
    TAIL_NUMBER = currentD['TAIL_NUMBER']
    ORIGIN_AIRPORT = currentD['ORIGIN_AIRPORT']
    DESTINATION_AIRPORT = currentD['DESTINATION_AIRPORT']
    SCHEDULED_DEPARTURE  = currentD['SCHEDULED_DEPARTURE']
    DEPARTURE_TIME = currentD['DEPARTURE_TIME']
    DEPARTURE_DELAY = currentD['DEPARTURE_DELAY']
    TAXI_OUT = currentD['TAXI_OUT']
    WHEELS_OFF = currentD['WHEELS_OFF']
    SCHEDULED_TIME = currentD['SCHEDULED_TIME']
    ELAPSED_TIME = currentD['ELAPSED_TIME']
    AIR_TIME  = currentD['AIR_TIME']
    DISTANCE  =  currentD['DISTANCE']
    WHEELS_ON = currentD['WHEELS_ON']
    TAXI_IN = currentD['TAXI_IN']
    SCHEDULED_ARRIVAL = currentD['SCHEDULED_ARRIVAL']
    ARRIVAL_TIME  = currentD['ARRIVAL_TIME']
    ARRIVAL_DELAY  = currentD['ARRIVAL_DELAY']
    DIVERTED    =  currentD['DIVERTED']
    CANCELLED   =  currentD['CANCELLED']
    CANCELLATION_REASON = currentD['CANCELLATION_REASON']
    AIR_SYSTEM_DELAY = currentD['AIR_SYSTEM_DELAY']
    SECURITY_DELAY   = currentD['SECURITY_DELAY']
    AIRLINE_DELAY  = currentD['AIRLINE_DELAY']
    LATE_AIRCRAFT_DELAY = currentD['LATE_AIRCRAFT_DELAY']
    WEATHER_DELAY = currentD['WEATHER_DELAY']
    rds_host  = "appychip-instance-1.ckbuxwoy9jlx.us-east-2.rds.amazonaws.com"
    name = "appychip"
    password = "appychip"
    db_name = "appychip"
    connection = pymysql.connect(rds_host, user=name, passwd=password, db=db_name)
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM flights")
    insertDB = '''INSERT INTO flights (YEAR, MONTH, DAY, DAY_OF_WEEK, AIRLINE, FLIGHT_NUMBER , TAIL_NUMBER, ORIGIN_AIRPORT, DESTINATION_AIRPORT, SCHEDULED_DEPARTURE,                                                                                                                       
    DEPARTURE_TIME, DEPARTURE_DELAY, TAXI_OUT, WHEELS_OFF, SCHEDULED_TIME, ELAPSED_TIME, AIR_TIME, DISTANCE, WHEELS_ON, TAXI_IN,                                                                                                                                                            
    SCHEDULED_ARRIVAL, ARRIVAL_TIME, ARRIVAL_DELAY, DIVERTED, CANCELLED, CANCELLATION_REASON, AIR_SYSTEM_DELAY, SECURITY_DELAY,                                                                                                                                                             
    AIRLINE_DELAY, LATE_AIRCRAFT_DELAY, WEATHER_DELAY) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
    val = (YEAR, MONTH, DAY, DAY_OF_WEEK, AIRLINE, FLIGHT_NUMBER , TAIL_NUMBER, ORIGIN_AIRPORT, DESTINATION_AIRPORT, SCHEDULED_DEPARTURE,
    DEPARTURE_TIME, DEPARTURE_DELAY, TAXI_OUT, WHEELS_OFF, SCHEDULED_TIME, ELAPSED_TIME, AIR_TIME, DISTANCE, WHEELS_ON, TAXI_IN,
    SCHEDULED_ARRIVAL, ARRIVAL_TIME, ARRIVAL_DELAY, DIVERTED, CANCELLED, CANCELLATION_REASON, AIR_SYSTEM_DELAY, SECURITY_DELAY,
    AIRLINE_DELAY, LATE_AIRCRAFT_DELAY, WEATHER_DELAY)
    cursor.execute(insertDB , val)
    connection.commit()
    cnt = cursor.rowcount
    connection.close()
    currentD["count"] = cnt
    resp = {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": "*",
        },
        "body": "\n"+(str(currentD))

    }

    return resp
