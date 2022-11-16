#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thur Nov  11 17:39:01 2022

@author: Mochet
"""
import requests

import mysql.connector as mc
from mysql.connector import Error

#API

#https://nominatim.openstreetmap.org/search?q=135+pilkington+avenue,+birmingham&format=xml&polygon_geojson=1&addressdetails=1





try:
    #MySQL database connection
    cnx=mc.connect(user='root',password="******",host='localhost',database='dataengineer')
    cnx.autocommit=True
    #selecting 'adress' table from dataengineer
    sql_Query = "select * from address"
    cursor = cnx.cursor(buffered=True)
    cursor.execute(sql_Query)
    records = cursor.fetchall()
    print("Connection started, Total number of rows in table: ", cursor.rowcount)
    
    #Adding lat & lon columnns (to run once)
   # cursor.execute("ALTER TABLE address ADD lat float;")
   # cursor.execute("ALTER TABLE address ADD lon float;")
    
    
    for row in records:
        
        #temporarly stocking needed values
        adress_id= row[0] 
        adress=row[1]
        city=row[2]
        postalcode=row[3]
        
        #creating API request
        req = ("https://nominatim.openstreetmap.org/search?" +
        "street="+adress +
        "&city=" + city +
        "&postalcode=" +postalcode +
        "&format=json" +
        "&polygon_svg=1"
        )
        
        #GET request
        result = requests.get(req)
        
        #adding the result to newly created columns
        if len(result.json())>0:
           
           cursor.execute("update address set lat="+str(result.json()[0]["lat"])+" where address_id="+str(adress_id)+";")
           cursor.execute("update address set lon="+str(result.json()[0]["lon"])+" where address_id="+str(adress_id)+";")
  
        #if API can't find the adress 
        else :
            cursor.execute("update  address set lat=0 where address_id="+str(adress_id)+";")
            cursor.execute("update  address set lon=0 where address_id="+str(adress_id)+";")
           

#error handling
except Error as e:
    print("Error while connecting to MySQL", e)


#closing connection and cursor
finally:
    if cnx.is_connected():
        cnx.close()
        cursor.close()
        print("MySQL connection is closed")

