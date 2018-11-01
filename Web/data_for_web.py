#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
@author: fanleehao
@contact: fanleehao@gmail.com
@file: data_for_web.py
@time: 2018/10/30 15:09
@desc:
'''

import pandas as pd

def set_name(house_info):
    len = house_info.index(',')
    src = house_info[0:len]
    district = {"JiangNing": "江宁区",
                "XuanWu": "玄武区",
                "YuHuaTai": "雨花台区",
                "LiShui": "溧水区",
                "LiuHe": "六合区",
                "QinHuai": "秦淮区",
                "JianYe": "建邺区",
                "GuLou": "鼓楼区",
                "QiXia": "栖霞区",
                "Pukou": "浦口区"
                }
    return district[src]


if __name__ == '__main__':

    # 将Streaming的结果预处理，去括号等
    source_name = "../data/streaming_result.csv"
    target_name = "../data/puredData.csv"

    with open(source_name, 'r', encoding="utf-8") as input_file:
        with open(target_name, 'w', encoding="utf-8") as output_file:
            output_file.write("date,name,type,price,count\n")
            for line in input_file:
                temp = line.replace('(', '').replace(')', '').replace('\'', '').replace(' ', '')[1:]
                # 这个日期后续可以根据原数据的文件名来获取
                line_to_write = "2018-10-30," + set_name(temp) + ',' + temp
                output_file.write(line_to_write)

    # 给JS调用处理文件数据
    df = pd.read_csv(target_name)

    # 这里50是计算结果中的总行数，加40 是为了平衡数据均值
    for i in range(50):
        df['price'][i] = df['price'][i] / df['count'][i] + 40
    # 价格数据
    df_price = pd.DataFrame(df, columns=['name', 'type', 'price', 'date'])
    df_price.to_csv('../data/price.csv', index=False, header=['name', 'type', 'value', 'date'])
    # 数量数据
    df_count = pd.DataFrame(df, columns=['name', 'type', 'count', 'date'])
    df_count.to_csv('../data/count.csv', index=False,  header=['name', 'type', 'value', 'date'])
