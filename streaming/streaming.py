
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession

'''
窗口函数的状态操作
https://blog.csdn.net/qq_35394891/article/details/82588275

'''

class utils():

    def __init__(self):
        pass

    def getSparkContext(self,appName, master, loglevel="WARN"):
        conf = SparkConf()
        conf.setAppName(appName).setMaster(master)
        sc = SparkContext(conf=conf)
        sc.setLogLevel(loglevel)
        return sc

    def getSparkSession(self,appName, master, loglevel="WARN"):

        sc = self.getSparkContext(self,appName, master, loglevel="WARN")
        spark = SparkSession(sparkContext=sc)
        return spark

u = utils()

if __name__=="__main__":

    sc=u.getSparkContext(appName="streaming",master="local[1]",loglevel="error")

    ssc=StreamingContext(sparkContext=sc,batchDuration=2)

    ssc.checkpoint("./chepoint")

    lines_stream=ssc.socketTextStream("localhost",9999)

    words_stream=lines_stream.flatMap(lambda x:x.split(" "))
    kv_words = words_stream.map(lambda x: (x, 1))

    # word_count=words.map(lambda x:(x,1)).reduceByKey(lambda x1,x2:x1+x2)
    # word_count.pprint()

    window_words = kv_words.reduceByKeyAndWindow(lambda a,b:a+b,lambda m,n:m-n,4,2)
    window_words.pprint()

    ssc.start()
    ssc.awaitTermination()