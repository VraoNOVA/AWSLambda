import json

import ast

import time
from time import gmtime, strftime

import pymysql

from DBUtils.PooledDB import PooledDB

#Database connection and pooling setup 
rds_host  = "compugain1.cftwo5jbmlrm.us-east-1.rds.amazonaws.com"
name = "compuGain1"
password = "compuGain1"
db_name = "compuGain1"
mySQLConnectionPool = PooledDB(creator   = pymysql,     

                               host      = rds_host,

                               user      = name,

                               password  = password,

                               database  = db_name,

                               autocommit    = True,

                               charset       = "utf8mb4",

                               cursorclass   = pymysql.cursors.DictCursor,

                               blocking      = True,

                               maxconnections = 5000)
                               
#Database conneciton setup ending 

def lambda_handler(event, context):
  try:
    #*****************************************************************
    #Initializing all the data we will place into the DB
    millis = int(round(time.time() * 1000))
    key1 = "insertTime"
    currentD =  (ast.literal_eval(str(event["body"])))
    currentD[key1] = str(millis)
    global mySQLConnectionPool
    one = mySQLConnectionPool.connection()
    cursorOne = one.cursor()
    cursorOne.execute("SELECT * FROM flights")
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
    insertDB = '''INSERT INTO flights (YEAR, MONTH, DAY, DAY_OF_WEEK, AIRLINE, FLIGHT_NUMBER , TAIL_NUMBER, ORIGIN_AIRPORT, DESTINATION_AIRPORT, SCHEDULED_DEPARTURE,                                                                                                                       
    DEPARTURE_TIME, DEPARTURE_DELAY, TAXI_OUT, WHEELS_OFF, SCHEDULED_TIME, ELAPSED_TIME, AIR_TIME, DISTANCE, WHEELS_ON, TAXI_IN,                                                                                                                                                            
    SCHEDULED_ARRIVAL, ARRIVAL_TIME, ARRIVAL_DELAY, DIVERTED, CANCELLED, CANCELLATION_REASON, AIR_SYSTEM_DELAY, SECURITY_DELAY,                                                                                                                                                             
    AIRLINE_DELAY, LATE_AIRCRAFT_DELAY, WEATHER_DELAY) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
    val = (YEAR, MONTH, DAY, DAY_OF_WEEK, AIRLINE, FLIGHT_NUMBER , TAIL_NUMBER, ORIGIN_AIRPORT, DESTINATION_AIRPORT, SCHEDULED_DEPARTURE,
    DEPARTURE_TIME, DEPARTURE_DELAY, TAXI_OUT, WHEELS_OFF, SCHEDULED_TIME, ELAPSED_TIME, AIR_TIME, DISTANCE, WHEELS_ON, TAXI_IN,
    SCHEDULED_ARRIVAL, ARRIVAL_TIME, ARRIVAL_DELAY, DIVERTED, CANCELLED, CANCELLATION_REASON, AIR_SYSTEM_DELAY, SECURITY_DELAY,
    AIRLINE_DELAY, LATE_AIRCRAFT_DELAY, WEATHER_DELAY)
    cursorOne.execute(insertDB , val)
    cursorOne.close()
    one.close()
    resp = {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": "*",
        },
        "body": "\n"+(str(currentD))

    }

    return resp
    
except Exception as e:
s = str(e)
print(s)
result = {
        "statusCode": 403,
        "headers": {
            "Access-Control-Allow-Origin": "*",
        },
        "body": s 
                    }
return result 
