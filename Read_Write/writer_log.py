#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
@author: fanleehao
@contact: fanleehao@gmail.com
@file: writer_log.py
@time: 2018/10/25 16:13
@desc:
'''

import os
import io
import time

i = 0
with open("../test_log.txt", 'w') as f:
    for i in range(100):
        f.writelines(str(i) + '\n')
        time.sleep(1)

