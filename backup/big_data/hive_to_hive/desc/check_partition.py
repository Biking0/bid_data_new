# encoding=utf8

import os
import sys


# check_partition.py

def desc_parser(line):
    desc_list = open('./desc.txt', 'r').readlines()

    result_list = []

    for i in range(len(desc_list)):

        # 忽略其他行
        if desc_list[i][0] == '+':
            continue
        line_list = desc_list[i].strip().replace(' ', '').replace('\t', '').replace('\n', '').split('|')

        # 忽略表头
        if line_list[1] == 'col_name' or 'NULL' in line_list[1]:
            continue

        if 'Partition' not in line_list[1]:
            print line_list[1], line_list[2], line_list[3],
            print '#'

        # 检测分区数量
        if desc_list[i][2] == '#':
            check_partition_list = desc_list[i].split(' ')

            if check_partition_list[2] == 'Partition':
                print '### 分区键'

                partition_list = []
                for j in range(i + 1, len(desc_list)):

                    # 忽略其他行
                    if desc_list[j][0] == '+':
                        continue

                    if desc_list[j][3] == ' ':
                        continue
                    partition_key = desc_list[j].split(' ')[1]
                    print partition_key
                    partition_list.append(partition_key)

                if len(partition_list) > 1:
                    print '### 多个分区', line, partition_list
                    check_result = open('/home/hive/hyn/hive_to_hive/desc/check_result.txt', 'a+')
                    check_result.write(line + ' ' + str(partition_list) + '\n')
                    check_result.close()

                else:
                    print '### 1个分区', line, partition_list
                    check_result = open('/home/hive/hyn/hive_to_hive/desc/check_result.txt', 'a+')
                    check_result.write(line + ' ' + str(partition_list) + '\n')
                    check_result.close()


            else:
                print line, '无分区'

            # 重要勿删，上一步分区已遍历完
            break


def read_table_name():
    f = open('./test_table_name.txt', 'r')
    i = 1
    for line in f.readlines():
        line = line.strip('\n')

        print 1, ' #########################'
        print line

        # 生产环境
        desc_sh = "beeline -u 'jdbc:hive2://192.168.190.88:10000/csap' -n hive -p %Usbr7mx -e 'desc  " + line + ' \' > ./desc.txt'

        # 测试环境
        #desc_sh = "beeline -u 'jdbc:hive2://172.22.248.19:10000/default' -n csap -p @WSX2wsx -e 'desc  " + line + ' \' > ./desc.txt'

        print desc_sh
        os.popen(desc_sh).readlines()
        desc_parser(line)


read_table_name()
