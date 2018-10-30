#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
@author: fanleehao
@contact: fanleehao@gmail.com
@file: dataPro.py
@time: 2018/10/30 10:23
@desc: 数据预处理
'''

import csv
import pandas as pd

if __name__ == '__main__':
    df = pd.read_csv('../data/lianjia.csv')
    df = pd.DataFrame(df, columns=['location_district', 'avg_price'])
    df_sample = df.sample(frac=1)
    # df_to_save.to_csv('../data/result.csv', index=False, header=None)
    # print(df_to_save.shape)   (23578, 2)
    width = df.shape[0] // 20
    # print(width)
    for i in range(20):
        j = 0
        df_to_save = df_sample[j:j + width]
        j = j + width
        df_to_save.to_csv('../data/dataSet' + str(i) + '.csv', index=False, header=None)


