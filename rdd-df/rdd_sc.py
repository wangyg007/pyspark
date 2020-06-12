
from pyspark import *
import time
import spark_test

if __name__=="__main__":

    spark_conf = SparkConf()
    spark_conf.setAppName("rdd-df").setMaster("local[2]")

    #pyspark程序的入口
    sc = SparkContext(conf=spark_conf)
    sc.setLogLevel("WARN")


    rdd = sc.parallelize(['a b c d', 'ff b hg ee', 'v b e a', 'a d f aa'])
    rdd.flatMap(lambda x:x.split(" ")).map(lambda x:(x,1)).reduceByKey(lambda x1,x2:x1+x2)\
        .sortBy(lambda x:x[1]).foreach(lambda x:print(x))


    time.sleep(10)