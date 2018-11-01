# CCTask
From Class——Cloud Compute @ NJU.2018

### 说明
 - 由于服务器集群的内存有限，一般将执行和提交的任务放在slave2机器上进行提交；否则运行会超时或内存爆掉
 - Python保存RDD文件到本地的API暂时没有？保存在HDFS时的数据可能会存在多层目录中，且文件名含有时间戳
 
### 目录
 - data：数据存储路径，包括爬虫、预处理、Streaming结果、供前端调用的数据
 - dfsIO：作业主脚本，分别写HDFS和监控HDFS及其上的Streaming计算
 - Spider：爬虫脚本
 - Web：前端展示
 
### 鸣谢（TEAMWORK）
 - 樊惠良
 - 冯雪松
 - 冯海涛

 - [可视化支持](https://github.com/Jannchie/Historical-ranking-data-visualization-based-on-d3.js.git)