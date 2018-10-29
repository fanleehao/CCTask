from pyspark import SparkContext
from pyspark.streaming import StreamingContext

def updateFunction(newValues, runningCount):
    if runningCount is None:
        runningCount = 0
    return sum(newValues, runningCount)

sc=SparkContext("local[2]","ziru")
ssc=StreamingContext(sc,35)
ssc.checkpoint(".")
lines = ssc.textFileStream("hdfs://localhost:9000/")
arrays=lines.map(lambda line:line.split(" "))
pairs=arrays.map(lambda word:(word[0],float(word[1])))
#pricesum=pairs.reduceByKey(lambda x,y:x+y)
pricesum=pairs.updateStateByKey(updateFunction)
places=arrays.map(lambda word:(word[0],1))
placecount=places.updateStateByKey(updateFunction)
#wordcounts.saveAsTextFiles("/tmp/wordcounts")
pricesum.pprint()
placecount.pprint()
#print(wordcounts.count())
ssc.start()
ssc.awaitTermination()
