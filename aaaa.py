#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
@author: fanleehao
@contact: fanleehao@gmail.com
@file: aaaa.py
@time: 2018/10/30 15:09
@desc:
'''

import pandas as pd

if __name__ == '__main__':
    df = pd.read_csv('result3.csv')
    df1 = pd.DataFrame(df, columns=[' name', ' type', ' value', 'data'])
    df1.to_csv('result4.csv', index=False)
