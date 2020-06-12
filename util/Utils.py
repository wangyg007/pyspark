
from pyspark import *
import sys
from pyspark.sql import SparkSession
import time


def getSparkContext(appName,master,loglevel="WARN"):
    conf = SparkConf()
    conf.setAppName(appName).setMaster(master)
    sc = SparkContext(conf=conf)
    sc.setLogLevel(loglevel)
    return sc

def getSparkSession(appName,master,loglevel="WARN"):
    sc = getSparkContext(appName,master,loglevel="WARN")
    spark = SparkSession(sparkContext=sc)
    return spark
