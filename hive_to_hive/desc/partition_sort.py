# encoding=utf8

# partition_sort.py
import os
import sys


# 统计分区字段种类
def save_partition_sort():

    # 分区种类结果统计
    partition_result_list = []

    # 临时存储
    partition_name_list = []

    # 读取分区结果文件
    partition_sort_list = open('./check_result.txt', 'r').readlines()
    for i in range(len(partition_sort_list)):

        print partition_sort_list[i]

        partition_str_list = partition_sort_list[i].split(' ')

        print partition_str_list

        # 检测多个分区，目前没有多分区表
        # if len(partition_str_list) > 1:
        #     print partition_sort_list[i]
        #     # break

        table_name = partition_str_list[0]

        partition_name = partition_str_list[1].replace('[', '').replace(']', '').replace('\'', '').replace('\n', '')

        print '# partition_name',partition_name,partition_name_list

        if partition_name in partition_name_list:
            continue

        # 根据分区名进行去重
        partition_name_list.append(partition_name)

        partition_result_list.append([table_name, partition_name])
        # if partition_name not in partition_result_list[i][1]:
        #     partition_result_list.append([table_name, partition_name])

        # 分区去重
        # flag = 0
        # for j in range(len(partition_result_list)):
        #
        #     if partition_result_list[j][1] == partition_name:
        #         flag = 1
        #         break
    print '分区种类：', len(partition_result_list)
    print partition_name_list
    for i in partition_result_list:
        print i
    # print partition_result_list


save_partition_sort()
