#!/usr/bin/env python
# -*-coding:utf-8 -*-
# 20210225

import requests
from lxml import etree
import re

url = "http://wquan.moojing.com/quiz/index.html"


# 解析item list
def item_list():
    res = requests.get(url)

    html = etree.HTML(res.text)

    result = etree.tostring(html)

    item_list = html.xpath('//div/li/text()')

    print('1. 解析item list:')
    print(item_list)


def ajax_result():
    ajax_url = "http://wquan.moojing.com/get_data?itemid=595843737123&time=1614185282440&sign=:%3E:=98%3C8%3C678jfx~6;696=:7=7995"

    res = requests.get(ajax_url)

    print('\n2. 解析ajax:')
    print(res.text)
    print(re.findall(r"skuId=(.+?)&", res.text))


item_list()
ajax_result()
