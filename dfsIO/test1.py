
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

def tpprint(val, num=100):
    """
    Print the first num elements of each RDD generated in this DStream.
    @param num: the number of elements from the first will be printed.
    """
    def takeAndPrint(time, rdd):
        taken = rdd.take(num + 1)
        print("########################")
        print("Time: %s" % time)
        print("########################")
        # DATEFORMAT = '%Y%m%d'
        # today = datetime.datetime.now().strftime(DATEFORMAT)
        myfile = open("/ttttt.txt", "w")
        for record in taken[:num]:
            print(record)
            myfile.write(str(record)+"\n")
        myfile.close()
        if len(taken) > num:
            print("...")
        print("")

    val.foreachRDD(takeAndPrint)






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
#res.repartition(1).saveAsTextFiles("/tmp/result.txt")
res.pprint()

print "\nthis is a line\n"
tpprint(res)

ssc.start()
ssc.awaitTermination()

