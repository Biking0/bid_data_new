# import vertica_python
#
# conn_info = {'host': '172.19.74.63',
#              'port': 5433,
#              'user': 'csapdwd',
#              'password': 'qaQA#3e4r1',
#              'database': 'csapdwd',
#              # 'backup_server_node': ['123.456.789.123', 'invalid.com', ('10.20.82.77', 6000)]
#              }
# connection = vertica_python.connect(**conn_info)
#
# cur = connection.cursor()
#
# cur.execute("SELECT * FROM columns LIMIT 2")
#
# print cur.fetchall()


a=[1,2]

print len(a)