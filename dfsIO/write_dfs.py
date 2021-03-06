#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: fanleehao
@contact: fanleehao@gmail.com
@file: write_dfs.py
@time: 2018/10/28 14:47
@desc: 写入hdfs文件
"""

import sys
from hdfs.client import Client
import time
import random

reload(sys)
sys.setdefaultencoding("utf-8")



class MyClient(object):

    def __init__(self, dfs_url):
        self.client = Client(dfs_url, timeout=200)

    # 读取hdfs文件内容,将每行存入数组返回
    def read_hdfs_file(self, filename):
        # with client.read('samples.csv', encoding='utf-8', delimiter='\n') as reader:
        #  for line in reader:
        # pass
        lines = []
        with self.client.read(filename, encoding='utf-8', delimiter='\n') as reader:
            for line in reader:
                # pass
                # print line.strip()
                lines.append(line.strip())
        return lines

    # 创建目录
    def mkdirs(self, hdfs_path):
        self.client.makedirs(hdfs_path)

    # 删除hdfs文件
    def delete_hdfs_file(self, hdfs_path):
        self.client.delete(hdfs_path)

    # 上传文件到hdfs
    def put_to_hdfs(self, local_path, hdfs_path):
        self.client.upload(hdfs_path, local_path, overwrite=True, cleanup=True)

    # 从hdfs获取文件到本地
    def get_from_hdfs(self, hdfs_path, local_path):
        # download(hdfs_path, local_path, overwrite=False)
        pass

    # 追加数据到hdfs文件
    def append_to_hdfs(self, hdfs_path, data):
        self.client.write(hdfs_path, data, overwrite=False, append=True)

    # 覆盖数据写到hdfs文件
    def write_to_hdfs(self, hdfs_path, data):
        self.client.write(hdfs_path, data, overwrite=True, append=False)

    # 移动或者修改文件
    def move_or_rename(self, hdfs_src_path, hdfs_dst_path):
        self.client.rename(hdfs_src_path, hdfs_dst_path)

    # 返回目录下的文件
    def list(self, hdfs_path):
        return self.client.list(hdfs_path, status=False)


if __name__ == '__main__':
    client = MyClient("http://master:50070")
    # print client.list('/')

    # 20为写入文件个数
    for j in range(20):
        filename = "../data/dataSet" + str(j) + ".csv";
        #with open(filename, 'w') as f:
        #    for i in range(100):
        #        avgPrice = random.uniform(150, 400)
        #        district = random.choice(("GuLou", "YuHuaTai", "JianYe", "QiXia", "XuanWu", "JiangNing", "QinHuai"))
        #        f.write(district + " " + str(avgPrice) + '\n')

        # 写hdfs文件
        print("Start to write " + str(j))
        client.put_to_hdfs(filename, '/')
        #print client.list("/")
        print("Successfully Writen " + str(j))
        time.sleep(10)
        print("sleep 10s done!")
