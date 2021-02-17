
test_str='["c88463b8-6c5c-4fab-b41c-685cbfb7db01","hour","23/0","23/0","23/0","23/0","23/0","23/0","23/0","23/0","23/0","23/0","23/0","23/0","23/0","23/0","23/0","23/0","23/0","23/0","23/0","23/0","23/0","23/0","23/0","23/0"]'

print (test_str)
list_str=[]

for i in range(len(test_str)):
	if test_str[i]=='"':
		# print(i)
		for j in range(i+1,len(test_str)):
			if test_str[j]=='"':
				#print (j)
				#print (test_str[i+1:j])
				target_str=test_str[i+1:j]
				
				if target_str ==',' or target_str == '':
					break
				
				list_str.append(target_str)
				
				
				
				break

print(list_str)