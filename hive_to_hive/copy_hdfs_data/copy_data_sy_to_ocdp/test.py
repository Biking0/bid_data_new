# random_str = ''.join(random.sample(string.ascii_letters + string.digits, 10))
#
# file_name = table_name + '_' + partition_date + '_' + random_str + '.txt'
#
# print file_name
#
# check_date_sql_sh = mysql_sh + check_date_sql + '\' > ' + get_task_file


# os.popen(get_task_sql_sh)

# get_task_list = open(get_task_file, 'r')

multi_list = []

# for table_name in get_task_list.readlines():
#     table_name = table_name.strip('\n').replace('\t', '').replace(' ', '')
#
#     print 1, ' #########################'
#     print table_name
#
#     multi_list.append(table_name)
#
#     input_date(table_name)

import datetime as date_time



print str(date_time.datetime.now())[11:13]