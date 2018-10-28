from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc=SparkContext("local[2]","ziru")
ssc=StreamingContext(sc,35)
lines = ssc.textFileStream("hdfs://localhost:9000/")
arrays=lines.map(lambda line:line.split(" "))
pairs=arrays.map(lambda word:(word[0],float(word[1])))
pricesum=pairs.reduceByKey(lambda x,y:x+y)

places=pairs.keys()
places=places.map(lambda place:(place,1))
placecount=places.reduceByKey(lambda x,y:x+y)
#输出文件，前缀+自动加日期
#wordcounts.saveAsTextFiles("/tmp/wordcounts")
pricesum.pprint()
placecount.pprint()
#print(wordcounts.count())
ssc.start()
ssc.awaitTermination()
