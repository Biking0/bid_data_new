for host in {llhadoop254,llhadoop255,llhadoop256,llhadoop257,llhadoop258,llhadoop259,llhadoop260}
 do
 for port in {7000,7001,7002,7003,7004,7005,7006,7007,7008,7009,7010,7011,7012,7013,7014,7015,7016,7017,7018,7019,7020,7021,7022,7023,7024,7025,7026,7027,7028,7029}
 do
 redis-cli -c -h $host  -p $port keys "lacci_activity_*" | xargs -r  -t -n1 redis-cli -c -h $host -p $port del
 done
 done
