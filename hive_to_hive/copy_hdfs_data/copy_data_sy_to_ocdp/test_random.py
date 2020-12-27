#!/usr/bin/env python
# -*- coding: utf-8 -*-
# import random
# import string
#
# salt = ''.join(random.sample(string.ascii_letters + string.digits, 10))
# print salt


insert_table_sql = "insert into tb_copy_data_log (data_source,table_name,partition_type,partition_time,copy_status,chk_status,start_time,end_time) values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')" % (
        '1', '1', '1', '1', '1', '1', '1', '1', '1', '1')
print insert_table_sql

