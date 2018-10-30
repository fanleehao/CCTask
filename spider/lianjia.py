# -*- coding: utf-8 -*-
from enum import Enum
import requests
import time
import pymongo
from lxml import etree
from requests.exceptions import RequestException
import re


def get_one_page(url, location, page):
    '''获取单页源码'''
    try:
        if location != "":
            if page == 1:
                url = "https://nj.lianjia.com/" + str(location) + "/"
            else:
                url = "https://nj.lianjia.com/" + str(location) + "/" + "pg" + str(page) + "/"
        else:
            if page == 1:
                url = "https://nj.lianjia.com/" + str(location) + "/"
            else:
                url = "https://nj.lianjia.com/" + str(location) + "/" + "pg" + str(page) + "/"
        headers = {
            'Referer': 'https://nj.lianjia.com/',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0(WindowsNT6.3;Win64;x64)AppleWebKit/537.36(KHTML,likeGecko)Chrome/68.0.3440.106Safari/537.36'
        }
        res = requests.get(url, headers=headers)
        if res.status_code == 200:
            return res.text
        return None
    except RequestException:
        return None


def get_location_sets(url, initial_html, page):
    location_sets = []
    contentTree = etree.HTML(initial_html)
    location_wide = contentTree.xpath('//div[@id="filter-options"]/dl[1]/dd/div[1]/a/@href')
    for item in location_wide[1:]:
        initial_html = get_one_page(url, item, page)
        contentTree = etree.HTML(initial_html)
        location_narrow = contentTree.xpath('//div[@id="filter-options"]/dl[1]/dd/div[2]/a/@href')
        for tiny in location_narrow[1:]:
            location_sets.append(tiny)
    return location_sets


def parse_one_page(sourcehtml):
    '''解析单页源码'''
    contentTree = etree.HTML(sourcehtml)  # 解析源代码
    results = contentTree.xpath('//ul[@id="house-lst"]/li')  # 利用XPath提取相应内容
    for result in results[1:]:
        location_district = contentTree.xpath('//div[@id="filter-options"]/dl[1]/dd/div[1]/a[@class="on"]/text()')[0]
        location_street = contentTree.xpath('//div[@id="filter-options"]/dl[1]/dd/div[2]/a[@class="on"]/text()')[0]
        location_title = result.xpath("./div/div[1]/div/a/span/text()")[0]
        location_title = location_title[0:len(location_title) - 2]
        room_type = result.xpath("./div/div[1]/div/span[1]/span/text()")[0]
        room_type = room_type[0:len(room_type) - 2]
        room_area = result.xpath("./div/div[1]/div/span[2]/text()")[0]
        room_area = room_area[0:len(room_area) - 4]
        room_orientation = result.xpath("./div/div[1]/div/span[3]/text()")[0]
        price = result.xpath("./div/div[2]/div/span/text()")[0]
        avg_price = float(price) / float(room_area)
        data = {
            "location_district": location_district,
            "location_street": location_street,
            "location_title": location_title,
            "room_type": room_type,
            "room_area": room_area,
            "room_orientation": room_orientation,
            "price": price,
            "avg_price": avg_price
        }
        save_to_mongodb(data)


def get_pages(url, location):
    """得到总页数"""
    page = 1
    html = get_one_page(url, location, page)
    contentTree = etree.HTML(html)
    pages = str(contentTree.xpath('//div[@class="list-wrap"]/div/@page-data')[0][13:])
    pages = re.findall(r"(.+?),", pages)
    for item in pages:
        pages = int(item)
    return pages


def save_to_mongodb(result):
    """存储到MongoDB中"""
    # 创建数据库连接对象, 即连接到本地
    client = pymongo.MongoClient(host="localhost")
    # 指定数据库,这里指定ziroom
    db = client.ziroom
    # 指定表的名称, 这里指定roominfo
    db_table = db.lianjia
    try:
        # 存储到数据库
        if db_table.insert(result):
            print("---存储到数据库成功---", result)
    except Exception:
        print("---存储到数据库失败---", result)


def main():
    url = "https://nj.lianjia.com/"
    location = "zufang"
    pages = get_pages(url, location)
    initial_html = get_one_page(url, location, pages)
    location_sets = get_location_sets(url, initial_html, pages)
    print(location_sets)
    for item in location_sets:
        for page in range(1, pages + 1):
            html = get_one_page(url, item, page)
            parse_one_page(html)
    print("done")


if __name__ == '__main__':
    main()
    time.sleep(1)
