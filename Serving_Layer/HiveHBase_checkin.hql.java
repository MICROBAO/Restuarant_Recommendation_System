

--beeline
beeline 
!connect jdbc:hive2://localhost:10000/default 
add jar /home/cloudera/project/week1/hive-hcatalog-core-0.13.0.jar;

DROP TABLE IF EXISTS Checkin;
DROP TABLE IF EXISTS HiveToHBase_Checkin;

--Create HBase table from Hive 

CREATE TABLE HiveToHBase_Checkin(
    checkin_info MAP<STRING, INT>,
    type STRING,
    business_id STRING
    )
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = 'checkincf:checkin_info, checkincf:type, :key')
TBLPROPERTIES ('hbase.table.name' = 'HiveToHBase_Checkin');



--Create Yelp_Checkin in hive

CREATE EXTERNAL TABLE Checkin(
    checkin_info MAP<STRING, INT>,
    type STRING,
    business_id STRING
    )
COMMENT 'checin data'
ROW FORMAT SERDE 'com.cloudera.hive.serde.JSONSerDe'
LOCATION '/home/cloudera/project/week1';



--Interacting with data (FROM source_hive_table INSERT INTO TABLE my_hbase_table)

FROM Checkin INSERT INTO TABLE HiveToHBase_Checkin SELECT *;



