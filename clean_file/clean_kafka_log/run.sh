#!/usr/bin/env python
# -*-coding:utf-8 -*-
# ***************************************************************************
# 文件名称：run.sh
# 功能描述：将脚本拷贝到远程服务器，随后日志删除脚本
# 输 入 表：
# 输 出 表：
# 创 建 者：hyn
# 创建日期：20191010
# 修改日志：
# 修改日期：
# ***************************************************************************
# 程序调用格式：nohup sh run.sh >> nohup.out &
# ***************************************************************************

# 将脚本拷贝到远程服务器，随后执行脚本
scp clean_kafka_log.py 10.218.192.54:/home/ocdp/
scp clean_kafka_log.py 10.218.192.55:/home/ocdp/
scp clean_kafka_log.py 10.218.192.56:/home/ocdp/
scp clean_kafka_log.py 10.218.192.57:/home/ocdp/
scp clean_kafka_log.py 10.218.192.58:/home/ocdp/
scp clean_kafka_log.py 10.218.192.59:/home/ocdp/
scp clean_kafka_log.py 10.218.192.60:/home/ocdp/      

ssh 10.218.192.54 "python /home/ocdp/clean_kafka_log.py"
ssh 10.218.192.55 "python /home/ocdp/clean_kafka_log.py"
ssh 10.218.192.56 "python /home/ocdp/clean_kafka_log.py"
ssh 10.218.192.57 "python /home/ocdp/clean_kafka_log.py"
ssh 10.218.192.58 "python /home/ocdp/clean_kafka_log.py"
ssh 10.218.192.59 "python /home/ocdp/clean_kafka_log.py"
ssh 10.218.192.60 "python /home/ocdp/clean_kafka_log.py"
