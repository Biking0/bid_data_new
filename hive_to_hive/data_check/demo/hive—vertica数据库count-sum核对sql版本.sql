
1.Hive和vertica库中数据稽核按照增量和全量2种方式稽核，带有日期周期的分区名称表按照增量进行稽核；其他表按照全量进行稽核。稽核采用count，
sum数字字段进行核对。Hive采用全量核对的方案，vertica库只核对入接口数据，并将结果写入数据库中进行核对，核对无误后 方可进行割接。

# 建表
DROP TABLE IF EXISTS CHK_RESULT;
CREATE TABLE CHK_RESULT (
 DATA_SOURCE    STRING          COMMENT '数据源'
,DES_TBL        STRING          COMMENT '目标表名'
,CYCLICAL       STRING          COMMENT '周期'
,COUNT1         STRING          COMMENT '数据统计'
,SUM1           STRING          COMMENT '求和' sum()
,REMARK         STRING          COMMENT '备注'
,CHK_DT         int             COMMENT '检核时间'
)
COMMENT '数据质量检核结果表'
PARTITION BY (CHK_TD DATE)
ROW FORMAT DELIMITED FILEDS TERMINATED BY ' ';

#
INSERT INTO CHK_RESULT
PARTITION  (DATE)(
 DATA_SOURCE 
,DES_TBL     
,CYCLICAL    
,COUNT1      
,SUM1        
,REMARK      
,CHK_DT      
)
SELECT   
'MYSQL' AS DATA_SOURCE
,'表名' AS DES_TBL
,''     AS CYCLICAL
,'目标记录数[' ||A.NEW_NUM || '],源记录数[' || B.OLD_NUM|| ']' AS COUNT1
,'目标字段总和[' || A.NEW_SUM '],源字段总和' || B.OLD_SUM ']' AS SUM1
,''     AS REMARK
,NOW() AS CHK_DT
FROM (
SELECT COUNT(*) NEW_NUM,NVL(SUM(CASE WHEN Q.AA IS NOT NULL THEN Q.AA ELSE 0 END),0) AS NEW_SUM FROM 新系统的表  Q WHERE DATE = LOAD_DATE AND 1=1
)A ,(
SELECT COUNT(*) OLD_NUM,NVL(SUM(CASE WHEN W.AA IS NOT NULL THEN W.AA ELSE 0 END),0) AS OLD_SUM FROM 老系统的表  W WHERE 1=1 AND DATE = LOAD_DATE
)B
WHERE A.NEW_NUM = B.OLD_NUM;



