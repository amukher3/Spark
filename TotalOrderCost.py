# -*- coding: utf-8 -*-
"""
Created on Fri Jul 10 20:48:48 2020

@author: Abhishek Mukherjee
"""

from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("TotalOrderCost")
sc = SparkContext(conf = conf)

def parseLine(tempStr):
    temp=tempStr.split(',')
    customerID=temp[0]
    itemID=temp[1]
    cost=float(temp[2])
    
    return customerID,cost

input = sc.textFile("file:///SparkCourse/customer-orders.csv")
totalContent=input.map(parseLine)
wordCounts=totalContent.reduceByKey(lambda x,y:x+y) #Adds the value to every common key 
wordCountsFlipped = wordCounts.map(lambda x: (x[1], x[0])) #flips the key-value pair
wordCountsSorted=wordCountsFlipped.sortByKey()
print(wordCountsSorted.collect())

for i in wordCountsSorted.collect():
    print(i[1],i[0])