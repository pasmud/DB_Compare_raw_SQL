from django.shortcuts import render 
from django.http import HttpResponse,JsonResponse 
from rest_framework.decorators import api_view 
from rest_framework.response import Response 


from django.shortcuts import render, HttpResponse 
from django.db import connections 
import pandas as pd 
from django.db import connection 
from sqlalchemy import text 
import sqlalchemy as sa 
from sqlalchemy.ext.declarative import DeclarativeMeta 
from django.db import connection, transaction
from django.db import connection

import jsonify 

import decimal, datetime 
import urllib 
import json 
import urllib 

fullresult = {} 

cursor1= connections['primary'].cursor() 
cursor2 = connections['secondary'].cursor() 


def dbloop(cursor,results):
    list = []
    i = 0
    for row in results:
        dict = {} 
        field = 0
        while True:
            try:
                dict[cursor.description[field][0]] = str(results[i][field])
                field = field +1
            except IndexError as e:
                break
        i = i + 1
        element = [dict]
        list.append(element)
    return list


def dbcompare(subresult):
    
  
    length = len(subresult['dev'])

    missinglist = []
    i = 0
    
    while i < len(subresult['dev']):
        missing = {}
        for key,value in subresult['dev'][i][0].items():
            
            if subresult['dev'][i][0][key] != subresult['prd'][i][0][key]:
                missing[key] = subresult['dev'][i][0][key]
        i += 1
    
        element = [missing]
        missinglist.append(element)
    
    return missinglist     


def dbqueries(id):
    
    dbqueries =[]
    dbqueries['Shift'] = f"select * from HumanResources.Shift where ShiftID = {id}"
    dbqueries['Person'] = f"SELECT * FROM Person.Person where BusinessEntityID = {id}"
    
    return dbqueries


def action(name,query):
    
    query = query
    subresult = {}
    
    cursor = cursor1
    cursor.execute(query)
    results = cursor.fetchall()
    
    # print(cursor.description)
    # print(results.__len__())    
    
    list = dbloop(cursor1,results)
    fullresult[f'{name}_dev'] = list
    subresult ['dev'] = list
    
    cursor = cursor2
    cursor.execute(query)
    results = cursor.fetchall()
    
    
    list = dbloop(cursor,results)
    
    fullresult[f'{name}_prd'] = list
    subresult ['prd'] = list
    
    # print(subresult)
    
    missing = dbcompare(subresult)
    fullresult[f'{name}_dif'] = missing
    
    return None


@api_view(['GET']) 
def rawSql1(self):
    
    queries = dbqueries(1)
    
    print(queries)
    
    
    
    # query = "select top(10) * from Person.Person"
    # subresult = {}
    
    # cursor = cursor1
    # cursor.execute(query)
    # results = cursor.fetchall()
    
    # # print(cursor.description)
    # # print(results.__len__())    
    
    # list = dbloop(cursor1,results)
    # fullresult['dev'] = list
    # subresult ['dev'] = list
    
    # cursor = cursor2
    # cursor.execute(query)
    # results = cursor.fetchall()
    
    
    # list = dbloop(cursor,results)
    
    # fullresult['prd'] = list
    # subresult ['prd'] = list
    
    # # print(subresult)
    
    # missing = dbcompare(subresult)
    
    # fullresult['dbmissing'] = missing
    return Response(fullresult)
