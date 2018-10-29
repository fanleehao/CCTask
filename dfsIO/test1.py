
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

def updateFunction(newValues, runningCount):
    if runningCount is None:
        runningCount = 0
    return sum(newValues, runningCount)
sc=SparkContext("local[2]","ziru")
ssc=StreamingContext(sc,35)
ssc.checkpoint(".")
lines = ssc.textFileStream("hdfs://master:9000/")
arrays=lines.map(lambda line:line.split(" "))
pairs=arrays.map(lambda word:(word[0],float(word[1])))
pricesum=pairs.updateStateByKey(updateFunction)
places=arrays.map(lambda word:(word[0],1))
placecount=places.updateStateByKey(updateFunction)
#wordcounts.saveAsTextFiles("/tmp/wordcounts")

res=pricesum.join(placecount)
res.repartition(1).saveAsTextFiles("/tmp/result.txt")
res.pprint()


ssc.start()
ssc.awaitTermination()
