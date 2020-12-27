import datetime


date_test=datetime.datetime.today()

offset=datetime.timedelta(days=-1)

date_test1=(date_test+offset).strftime('%Y%m%d')

print date_test
print date_test1
print type(date_test)
print type(date_test1)
