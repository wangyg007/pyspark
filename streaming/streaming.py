
from pyspark import SparkContext
from pyspark.streaming import StreamingContext



if __name__=="__main__":

    sc=SparkContext("local[2]","streaming_test")
    sc.setLogLevel(logLevel="error")
    ssc=StreamingContext(sparkContext=sc,batchDuration=2)

    ssc.checkpoint("./chepoint")

    lines=ssc.socketTextStream("192.168.140.128",9999)

    words=lines.flatMap(lambda x:x.split(" "))
    kv_words = words.map(lambda x: (x, 1))
    # word_count=words.map(lambda x:(x,1)).reduceByKey(lambda x1,x2:x1+x2)
    # word_count.pprint()
    window_words = kv_words.reduceByKeyAndWindow(lambda a,b:a+b,lambda m,n:m-n,4,2)
    window_words.pprint()

    ssc.start()
    ssc.awaitTermination()