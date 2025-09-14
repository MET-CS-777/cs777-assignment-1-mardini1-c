from __future__ import print_function

import os
import sys
import requests
from operator import add
import shutil

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import *


#Exception Handling and removing wrong datalines
def isfloat(value):
    try:
        float(value)
        return True
 
    except:
         return False

#Function - Cleaning
#For example, remove lines if they don’t have 16 values and 
# checking if the trip distance and fare amount is a float number
# checking if the trip duration is more than a minute, trip distance is more than 0.1 miles, 
# fare amount and total amount are more than 0.1 dollars
def correctRows(p):
    if(len(p)==17):
        if(isfloat(p[5]) and isfloat(p[11])):
            if(float(p[4])> 60 and float(p[5])>0 and float(p[11])> 0 and float(p[16])> 0):
                return p

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: main_task1 <input_csv> <output_task1> <output_task2>", file=sys.stderr)
        sys.exit(-1)

    input_path = sys.argv[1]
    output_task1 = sys.argv[2]
    output_task2 = sys.argv[3]

    spark = SparkSession.builder.appName("Assignment-1").getOrCreate()
    sc = spark.sparkContext

    # Load and clean data
    rdd = sc.textFile(input_path)
    rows = rdd.map(lambda x: x.split(",")).filter(lambda x: correctRows(x) is not None)

    # Task 1: Top 10 taxis with most distinct drivers
    # Extract (taxi_id, driver_id) pairs
    taxi_driver = rows.map(lambda x: (x[0], x[1]))

    # Count the distinct drrivers and get the top 10
    driver_counts = taxi_driver.distinct().map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b)
    top10 = driver_counts.takeOrdered(10, key=lambda x: -x[1])

    results_1 = sc.parallelize(top10)
    # Save Task 1 output
    results_1.coalesce(1).saveAsTextFile(sys.argv[2])

    # Task 2: Top 10 drivers by earnings per minute
    # Calculate earnings per minute and sum them
    driver_metrics = rows.map(lambda x: (x[1], (float(x[16]), float(x[4]) / 60)))
    driver_totals = driver_metrics.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

    # Calculate to find the earnings per minute and return the top 10
    earnings_per_minute = driver_totals.mapValues(lambda x: x[0] / x[1])
    top_earners = earnings_per_minute.takeOrdered(10, key=lambda x: -x[1])

    results_2 = sc.parallelize(top_earners)
    # Save Task 1 output
    results_2.coalesce(1).saveAsTextFile(sys.argv[3])

    spark.stop()