#!/usr/bin/env python
# -*-coding:utf-8 -*-
#hqltools.py
from pyhs2.haconnection import HAConnection
import time
import math
import os,sys
import pyhs2

ISOTIMEFORMAT='%Y-%m-%d %X'
path = '/data1/Python_log/'
hosts=["10.218.9.13", "10.218.9.23"]
#conf = {"krb_host":"hadoop.hadoop.com", "krb_service":"hive"}


#conf = {"krb_host":"ocdp-hnbdcluster@HNBD.COM", "krb_service":"hive"}
conf = {"krb_host":"hnbd051", "krb_service":"hive"}

def HConn():
    conn = HAConnection(hosts = hosts,
                            port = 10000,
                            #authMechanism = "KERBEROS",
                            authMechanism = "KERBEROS",
							user='ocdp-hnbdcluster@HNBD.COM',
							#database='default',
                            configuration = conf,timeout=9999999999).getConnection()
    return conn

sql="show databases"
conn = HConn()
cur=conn.cursor()
result=cur.execute(sql)

print result