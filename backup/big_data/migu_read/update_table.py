


hive -e "set hive.exec.compress.output=false;set hive.cli.print.header=false; drop table kf_01_mass_hostlist_2020_week5;alter table dm_cmread_booklist rename to  kf_01_mass_hostlist_2020_week5; '"



drop table kf_01_mass_hostlist_2020_week5;alter table dm_cmread_booklist rename to  kf_01_mass_hostlist_2020_week5;	

load data local inpath '/home/ocetl/yfx/cmread_hotlist_20200424.txt' overwrite into table dm_cmread_booklist;


sh="hive -e 'set hive.exec.compress.output=false;set hive.cli.print.header=false; load data local inpath '/home/ocetl/yfx/cmread_hotlist_20200424.txt' overwrite into table dm_cmread_booklist; '"