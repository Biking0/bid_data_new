#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：selenium.py
# 功能描述：三期任务监控
# 输 入 表：tb_dwd_mid_ci_loc_merge_hour      
# 输 出 表：tb_dwd_ci_loc_merge_hour
# 创 建 者：chengpx
# 创建日期：2017年11月10日
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：python tb_dwd_ci_loc_merge_mc_hour.py 2015073001
# ***************************************************************************


#import requests
import selenium

from selenium import webdriver as we
import time,json

# url = 'https://wizzair.com/#/booking/select-flight/TIA/BUD/2018-12-22/null/1/0/0/0/null'
get_cookies_url = 'https://wizzair.com/en-gb#/booking/select-flight/TIA/DTM/2018-12-20/null/1/0/0/0/null'

driver1 = we.Chrome(executable_path='E:\soft\chromedriver.exe')


# driver=webdriver.Firefox(executable_path = 'D:\softInstall\Mozilla Firefox\geckodriver.exe')

# driver=webbrowser.Firefox(executable_path = 'D:\softInstall\Mozilla Firefox\geckodriver.exe')


# driver1.close()


url_search = "https://be.wizzair.com/9.1.0/Api/search/search"


class test(object):

    def __init__(self):
        pass

    def get_cookie(self):
        driver1 = we.Chrome(executable_path='E:\softInstall\chromedriver.exe')
        driver1.get(get_cookies_url)

        self.abck_list=[]
        for i in range(9):
            driver1.find_element_by_tag_name('main').click()
            print (1)

            self.abck = driver1.get_cookie('_abck').get('value')
            self.abck_list.append(self.abck)
            print (i, self.abck)
            if i!=0 and self.abck==self.abck_list[i-1]:
                return
            print (i, self.abck)


    def get_data(self):

        for i in self.abck_list:
            print (123)
            payload = "{\r\n\t\"isFlightChange\": false,\r\n\t\"isSeniorOrStudent\": false,\r\n\t\"flightList\": [{\r\n\t\t\"departureStation\": \"TIA\",\r\n\t\t\"arrivalStation\": \"DTM\",\r\n\t\t\"departureDate\": \"2018-12-22\"\r\n\t}],\r\n\t\"adultCount\": 1,\r\n\t\"childCount\": 0,\r\n\t\"infantCount\": 0,\r\n\t\"wdc\": true\r\n}"
            headers = {
                'accept': "application/json, text/plain, */*",
                'origin': "https://wizzair.com",
                'user-agent': "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36",
                'content-type': "application/json;charset=UTF-8",
                'referer': "https://wizzair.com/en-gb",
                'accept-encoding': "gzip, deflate, br",
                'accept-language': "zh-CN,zh;q=0.9",
                'cookie': '_abck=' + i,
                'cache-control': "no-cache",
                'postman-token': "55999a7b-9f88-8c8d-1a7a-6750c32175bd"
            }

            response = requests.request("POST", url_search, data=payload, headers=headers, verify=False)
            data_dict = json.loads(response.text)
            print(response.text)


    def run(self):
        self.get_cookie()
        for i in range(100):
            try:
                # self.get_cookie()

                # time.sleep(10)
                self.get_data()
                print (123)
            except:
                self.get_cookie()
                time.sleep(5)
                print ('get cookies')

start=test()

