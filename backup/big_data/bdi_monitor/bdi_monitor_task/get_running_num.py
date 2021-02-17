def get_running_num():
	
	#get_119chk_sh='hadoop fs -get /user/hive/warehouse/asiainfoh.db/bdi_monitor/119chk/* ./'
	#get_120chk_sh='hadoop fs -get /user/hive/warehouse/asiainfoh.db/bdi_monitor/120chk/* ./'
	
	#os.popen(get_119chk_sh).readlines()
	#os.popen(get_120chk_sh).readlines()
	
	rm_119chk_sh="hadoop fs -rm /user/hive/warehouse/asiainfoh.db/bdi_monitor/119chk/*"
	rm_120chk_sh="hadoop fs -rm /user/hive/warehouse/asiainfoh.db/bdi_monitor/120chk/*"
	
	#os.popen(rm_119chk_sh).readlines()
	#os.popen(rm_120chk_sh).readlines()
	
	f_119=open('./119.chk').readlines()
	
	print f_119
	print type(f_119)
	f_120=open('./120.chk').readlines()
	
	
	print f_119
	print type(f_119)

get_running_num()