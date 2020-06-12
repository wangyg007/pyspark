
from pyspark import *
import sys
from pyspark.sql import SparkSession
import time


if __name__=="__main__":

    conf = SparkConf()
    conf.setAppName("streaming").setMaster("local[2]")

    sc = SparkContext(conf=conf)
    sc.setLogLevel(logLevel="WARN")
    #
    # rdd1=sc.textFile("C:\\Users\\Administrator\\Desktop\\bsj\\xsss.data")
    # rdd1.flatMap(lambda x:x.split(" ")).map(lambda x:(x,1)).reduceByKey(lambda x1,x2:x1+x2).sortByKey().foreach(
    #     lambda x:print(x)
    # )

    spark = SparkSession(sparkContext=sc)
    print(spark)
    #builder.appName("test_sql").config("master", "local").getOrCreate
    #df=spark.read.json("C:\\Users\\Administrator\\Desktop\\pydata\\json.json")
    #df.filter(df["ssMoney"]>0).show()

    #df.createOrReplaceTempView("inout")
    #sqldf=spark.sql("select* from inout where ssMoney>0")
    #sqldf.show()

    #sqldf.rdd.foreach(lambda x:print(x))

    #rdddf=spark.createDataFrame(sqldf.rdd)
    #rdddf.foreach(lambda x:print(x))
    #rdddf.show()

    time.sleep(200)