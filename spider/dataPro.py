#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: fanleehao
@contact: fanleehao@gmail.com
@file: dataPro.py
@time: 2018/10/30 10:23
@desc: 数据预处理
"""

import csv
import pandas as pd

if __name__ == '__main__':
    df = pd.read_csv('../data/lianjia.csv')
    df = pd.DataFrame(df, columns=['location_district', 'avg_price'])
    df_sample = df.sample(frac=1)
    # df_to_save.to_csv('../data/streaming_result.csv', index=False, header=None)
    # print(df_to_save.shape)   (23578, 2)
    width = df.shape[0] // 5
    # print(width)
    j = 0

    # 将爬取获取的数据分成几片
    # 如果确实按天爬取，则不需要处理，按天为时间单位存储为单个的数据文件即可
    for i in range(5):
        df_to_save = df_sample[j:j + width]
        j = j + width
        df_to_save.to_csv('../data/dataSet' + str(i) + '.csv', index=False, header=None)


