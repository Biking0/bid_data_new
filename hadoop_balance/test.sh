#!/bin/bash
#作用：hdfs使用率取最大100个主机和最小80个主机进行数据均衡
#打印报告
hdfs dfsadmin -report>report.txt
#截取主机名
cat report.txt | grep -i "hostname" | awk -F ': ' '{print $2}' >hostname.txt
#截取hdfs使用率
cat report.txt | grep -i "DFS Used%" | awk -F ': ' '{print $2}' | awk -F '%' '{print $1}' >dfsused.txt
#截取datanode存活数
livenum=$(cat report.txt | grep -i "Live datanodes" | awk -F "(" '{print $2}' | awk -F ")" '{print $1}')
#删除总的hdfs使用率
sed -i '1d' dfsused.txt
#截取存活datanode的hdfs使用率和主机名
dfsarr=(sed -n '1,$livenum p' dfsused.txt)
hostarr=(sed -n '1,$livenum p' hostname.txt)
#组合主机名和hdfs使用率一一对应起来
let livenum+=1
if [ $livenum -gt  0 ]
then
   for((i=1;i<$livenum;i++))
   do
    for((j=1;j<$livenum;j++))
    do
      if [ $i -eq $j ]
      then
         echo  ${hostarr[$i]} ":" ${dfsarr[$j]} >> hostdfs.txt
      fi
    done
   done
else
    echo "Not Live DataNodes"
fi
#获取使用率最大100个主机名和最小80台主机名（按第二列排序）
sort -rn -k 2 -t : hostdfs.txt | awk -F ":" '{print $1}' | head -n 100 >>host.txt
sort -rn -k 2 -t : hostdfs.txt | awk -F ":" '{print $1}' | tail -n 80 >>host.txt
#求平均值和最大值进行比较
avg_used=$(cat hostdfs.txt | awk -F ":" '{print $2*100}' | awk '{sum+=$1} END {print sum/NR}')
max_used=$(cat hostdfs.txt | awk -F ":" '{print $2*100}' | sort -rn | head -n 1)
diff_max_avg=$(($max_used-$avg_used))

echo $diff_max_avg

#如果最大值与平均值的之差大于5，表示集群数据不均衡
if [ $diff_max_avg -gt 5 ]
then
    jps | grep -i "balancer"
    if [ $? -eq 0]
    then
       kill -9  $(jps | grep -i "balancer" | awk '{print $1}')
    else
       #对这些主机执行局部均衡
       hdfs   dfs  -rm   /system/balancer.id
       hdfs balancer
       -Ddfs.datanode.balance.max.concurrent.moves = 10 \
       -Ddfs.balancer.moverThreads = 1024 \
       -Ddfs.balance.bandwidthPerSec = 104857600 \
       -policy  datanode  -threshold  5  -include -f host.txt
    fi
else
    echo "Nothing to do"
fi