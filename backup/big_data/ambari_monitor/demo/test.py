import json

f=open('test.txt')
dict_data=json.load(f)

for i in dict_data['items']:
	print i['ServiceInfo']['service_name']