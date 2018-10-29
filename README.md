# CCTask
From Class——Cloud Compute AT NJU.

## 说明
 - 由于服务器集群的内存有限，一般将执行和提交的任务放在slave2机器上进行提交；否则运行会超时或内存爆掉
 - Python保存RDD文件到本地的API暂时没有？  保存在HDFS时的数据可能会存在多层目录中，且文件名含有时间戳