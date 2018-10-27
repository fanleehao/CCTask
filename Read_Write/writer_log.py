#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
@author: fanleehao
@contact: fanleehao@gmail.com
@file: writer_log.py
@time: 2018/10/25 16:13
@desc:
'''

import sys
from hdfs.client import Client

# 设置utf-8模式
reload(sys)
sys.setdefaultencoding("utf-8")


# 关于python操作hdfs的API可以查看官网:
# https://hdfscli.readthedocs.io/en/latest/api.html


# 读取hdfs文件内容,将每行存入数组返回
def read_hdfs_file(client, filename):
    # with client.read('samples.csv', encoding='utf-8', delimiter='\n') as reader:
    #  for line in reader:
    # pass
    lines = []
    with client.read(filename, encoding='utf-8', delimiter='\n') as reader:
        for line in reader:
            # pass
            # print line.strip()
            lines.append(line.strip())
    return lines


# 创建目录
def mkdirs(client, hdfs_path):
    client.makedirs(hdfs_path)


# 删除hdfs文件
def delete_hdfs_file(client, hdfs_path):
    client.delete(hdfs_path)


# 上传文件到hdfs
def put_to_hdfs(client, local_path, hdfs_path):
    client.upload(hdfs_path, local_path, cleanup=True)


# 从hdfs获取文件到本地
# def get_from_hdfs(client, hdfs_path, local_path):
#     download(hdfs_path, local_path, overwrite=False)


# 追加数据到hdfs文件
def append_to_hdfs(client, hdfs_path, data):
    client.write(hdfs_path, data, overwrite=False, append=True)


# 覆盖数据写到hdfs文件
def write_to_hdfs(client, hdfs_path, data):
    client.write(hdfs_path, data, overwrite=True, append=False)


# 移动或者修改文件
def move_or_rename(client, hdfs_src_path, hdfs_dst_path):
    client.rename(hdfs_src_path, hdfs_dst_path)


# 返回目录下的文件
def list(client, hdfs_path):
    return client.list(hdfs_path, status=False)


# client = Client(url, root=None, proxy=None, timeout=None, session=None)
client = Client("http://118.25.144.147:50070")
with client.read("/aaa") as reader:
    print reader.read()



# print client.list("/")
# move_or_rename(client,'/input/2.csv', '/input/emp.csv')
# print read_hdfs_file(client, '/aaa')
# put_to_hdfs(client,'/home/shutong/hdfs/1.csv','/input/')
# append_to_hdfs(client, '/aaa', 'nice you' + '\n')
# write_to_hdfs(client,'/input/emp.csv','我爱你'+'\n')
# read_hdfs_file(client,'/input/emp.csv')
# move_or_rename(client,'/input/emp.csv', '/input/2.csv')
# mkdirs(client,'/input/python')
# print list(client,'/input/')
# chown(client,'/input/1.csv', 'root')


