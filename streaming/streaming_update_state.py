
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession

'''
窗口函数都是有状态的操作
有状态操作是跨时间区间跟踪处理数据的操作。依赖于之前批次的数据。
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

def update_function(new_values,pre_sum):
    return sum(new_values)+(pre_sum or 0)


if __name__=="__main__":

    sc=u.getSparkContext(appName="streaming",master="local[2]",loglevel="error")

    ssc=StreamingContext(sparkContext=sc,batchDuration=2) #2s一个批次

    ssc.checkpoint("./chepoint")

    lines_stream=ssc.socketTextStream("localhost",9999)

    words_stream=lines_stream.flatMap(lambda x:x.split(" "))
    kv_words = words_stream.map(lambda x: (x, 1))

    state_words_count =  kv_words.updateStateByKey(update_function)

    state_words_count.pprint()

    state_words_count.pprint()


    ssc.start()
    ssc.awaitTermination()