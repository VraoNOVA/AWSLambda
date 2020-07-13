from profilehooks import profile
import pymysql
import sys
REGION = 'us-east-2'
rds_host  = "appychip-instance-1.ckbuxwoy9jlx.us-east-2.rds.amazonaws.com"
name = "appychip"
password = "appychip"
db_name = "appychip"
connection = pymysql.connect(rds_host, user=name, passwd=password, db=db_name)
cursor = connection.cursor()
cursor.execute("DROP TABLE flights")
cursor.execute(("""CREATE TABLE flights (YEAR VARCHAR(255), MONTH VARCHAR(255) , DAY VARCHAR(255) ,                                                                                                              
DAY_OF_WEEK VARCHAR(255) ,                                                                                                                                                                                       
AIRLINE VARCHAR(255) ,                                                                                                                                                                                           
FLIGHT_NUMBER VARCHAR(255) ,                                                                                                                                                                                     
TAIL_NUMBER VARCHAR(255) ,                                                                                                                                                                                       
ORIGIN_AIRPORT VARCHAR(255) ,                                                                                                                                                                                    
DESTINATION_AIRPORT VARCHAR(255) ,                                                                                                                                                                               
SCHEDULED_DEPARTURE VARCHAR(255) ,                                                                                                                                                                               
DEPARTURE_TIME VARCHAR(255) ,                                                                                                                                                                                    
DEPARTURE_DELAY VARCHAR(255) ,                                                                                                                                                                                   
TAXI_OUT VARCHAR(255) ,                                                                                                                                                                                          
WHEELS_OFF VARCHAR(255) ,                                                                                                                                                                                        
SCHEDULED_TIME VARCHAR(255) ,                                                                                                                                                                                    
ELAPSED_TIME VARCHAR(255) ,                                                                                                                                                                                      
AIR_TIME VARCHAR(255) ,                                                                                                                                                                                          
DISTANCE VARCHAR(255) ,                                                                                                                                                                                          
WHEELS_ON VARCHAR(255) ,                                                                                                                                                                                         
TAXI_IN VARCHAR(255) ,                                                                                                                                                                                           
SCHEDULED_ARRIVAL VARCHAR(255) ,                                                                                                                                                                                 
ARRIVAL_TIME VARCHAR(255)  ,                                                                                                                                                                                     
ARRIVAL_DELAY VARCHAR(255) ,                                                                                                                                                                                     
DIVERTED VARCHAR(255) ,                                                                                                                                                                                          
CANCELLED VARCHAR(255) ,                                                                                                                                                                                         
CANCELLATION_REASON VARCHAR(255) ,                                                                                                                                                                               
AIR_SYSTEM_DELAY VARCHAR(255) ,                                                                                                                                                                                  
SECURITY_DELAY VARCHAR(255) ,                                                                                                                                                                                    
AIRLINE_DELAY VARCHAR(255) ,                                                                                                                                                                                     
LATE_AIRCRAFT_DELAY VARCHAR(255) ,                                                                                                                                                                               
WEATHER_DELAY VARCHAR(255)  )"""))
number_of_rows = cursor.execute("SELECT * FROM flights")

insertDB = '''INSERT INTO flights (YEAR, MONTH, DAY, DAY_OF_WEEK, AIRLINE, FLIGHT_NUMBER , TAIL_NUMBER, ORIGIN_AIRPORT, DESTINATION_AIRPORT, SCHEDULED_DEPARTURE,                                                
DEPARTURE_TIME, DEPARTURE_DELAY, TAXI_OUT, WHEELS_OFF, SCHEDULED_TIME, ELAPSED_TIME, AIR_TIME, DISTANCE, WHEELS_ON, TAXI_IN,                                                                                     
SCHEDULED_ARRIVAL, ARRIVAL_TIME, ARRIVAL_DELAY, DIVERTED, CANCELLED, CANCELLATION_REASON, AIR_SYSTEM_DELAY, SECURITY_DELAY,                                                                                      
AIRLINE_DELAY, LATE_AIRCRAFT_DELAY, WEATHER_DELAY) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
val = ("2015","1","1","4","AS","98","N407AS","ANC","SEA","0005","2354","-11","21","0015","205","194","169","1448","0404","4","0430","0408","-22","0","0","","","","","","")
cursor.execute(insertDB , val)
print(cursor.rowcount, "record inserted.")
cursor.execute("")
