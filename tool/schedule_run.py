#!/usr/bin/env python
# -*-coding:utf-8 -*-
#********************************************************************************
# ** 文件名称：run_crontab.py
# ** 功能描述：咪咕阅读，定时每周五运行
# ** 输 入 表：
# ** 输 出 表：
# ** 创 建 者：hyn
# ** 创建日期：20200220
# ** 修改日志：
# ** 修改日期：20200524
# *******************************************************************************
# ** 程序调用格式：nohup python schedule_run.py >> /home/ocetl/hyn/miguread/log/schedule_run.log
# *******************************************************************************

import os
import time
import datetime

def run():
    pass

log_path = '/home/ocetl/hyn/miguread/log/'

while True:
    # 在周五指定时间运行
    today = datetime.datetime.now().weekday() + 1
    print today

    # 周五当天每小时检测是否到15点以后，非周五每5个小时检测一次
    if today == 4:
        while True:

            # 获取周五当天15点时间
            today_str = time.strftime('%Y%m%d', time.localtime(time.time()))

            is_runed= '/home/ocetl/hyn/miguread/log/migureading_running_' + today_str + '.log'
            print is_runed

            # 是否运行过
            if os.path.exists(is_runed):
                # 休息24小时
                print datetime.datetime.now()
                print 'is_runed sleep 24hour'
                time.sleep(86400)

                # 执行完程序退出
                break

            stamptime_15 = time.mktime(time.strptime(today_str + ' 15:00:00', '%Y%m%d %H:%M:%S'))

            # 获取当前时间戳
            stamptime_now = time.time()

            # 是否到当天15点以后
            print stamptime_15
            print stamptime_now
		
            if stamptime_now > stamptime_15:
                print 'start run'

                # 调用程序,python sh
                # python /home/ocetl/yfx/migureading_running.py 20200207
                # sh /home/ocetl/yfx/recommend/six.sh kf_01_mass_cmread_rec_2020_week5_1_six_20200207 20200207
                # sh /home/ocetl/yfx/recommend/ten.sh kf_01_mass_cmread_rec_2020_week5_1_20200207 20200207
                python_sh = 'nohup python /home/ocetl/yfx/migureading_running.py' + ' ' + today_str + ' > /home/ocetl/hyn/miguread/log/migureading_running_' + today_str + '.log'
                six_sh = 'nohup sh /home/ocetl/yfx/recommend/six.sh kf_01_mass_cmread_rec_2020_week5_1_six_' + today_str + ' ' + today_str + ' > /home/ocetl/hyn/miguread/log/six_' + today_str + '.log'
                ten_sh = 'nohup sh /home/ocetl/yfx/recommend/ten.sh kf_01_mass_cmread_rec_2020_week5_1_' + today_str + ' ' + today_str + ' > /home/ocetl/hyn/miguread/log/ten_' + today_str + '.log'

                print python_sh
                print six_sh
                print ten_sh

                # 测试
                # python1_sh = 'nohup python /home/ocetl/hyn/test/sleep.py > python1_sh.log'
                # sh1 = 'nohup sh /home/ocetl/hyn/test/sleep.sh > sh1.log'
                # sh2 = 'nohup sh /home/ocetl/hyn/test/sleep2.sh > sh2.log'
                # print python1_sh
                # os.popen(python1_sh)
                # print sh1
                # os.popen(sh1)
                # print sh2
                # os.popen(sh2)

                # 执行脚本
                
                os.popen(python_sh)
                os.popen(six_sh)
                os.popen(ten_sh)

                # 休息24小时
                print datetime.datetime.now()
                print 'sleep 24hour'
                time.sleep(86400)

                # 执行完程序退出
                break

            print 'sleep 30min'
            time.sleep(1800)
    print datetime.datetime.now()
    print '当天非周五'
    print 'sleep 5hour'
    time.sleep(18000)


# week = datetime.datetime.strptime("20200220", "%Y%m%d").weekday() + 1
# print week

