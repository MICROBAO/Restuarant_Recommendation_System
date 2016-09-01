

--beeline
beeline 
!connect jdbc:hive2://localhost:10000/default 
add jar /home/cloudera/project/week1/hive-hcatalog-core-0.13.0.jar;


DROP TABLE IF EXISTS Business;
DROP TABLE IF EXISTS HiveToHBase_Business;




--Create HBase table from Hive 

CREATE TABLE HiveToHBase_Business(
    business_id STRING,
    full_address STRING,
    open BOOLEAN,
    categories ARRAY<STRING>,
    city STRING,
    review_count INT,
    name STRING,
    neighborhoods ARRAY<STRING>,
    longitude DOUBLE,
    state STRING,
    stars DOUBLE,
    latitude DOUBLE,
    type STRING
    )
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key, businesscf:full_address, businesscf:open, businesscf:categories, businesscf:city, businesscf:review_count, businesscf:name, businesscf:neighborhoods, businesscf:longitude, businesscf:state, businesscf:stars, businesscf:latitude, businesscf:type')
TBLPROPERTIES ('hbase.table.name' = 'HiveToHBase_Business');



--Create Business in hive

CREATE EXTERNAL TABLE Business(
    business_id STRING,
    full_address STRING,
    open BOOLEAN,
    categories ARRAY<STRING>,
    city STRING,
    review_count INT,
    name STRING,
    neighborhoods ARRAY<STRING>,
    longitude DOUBLE,
    state STRING,
    stars DOUBLE,
    latitude DOUBLE,
    type STRING
    )
COMMENT 'businesss data'
ROW FORMAT SERDE 'com.cloudera.hive.serde.JSONSerDe'
LOCATION '/home/cloudera/project/week1';



--Interacting with data (FROM source_hive_table INSERT INTO TABLE my_hbase_table)

FROM Business INSERT INTO TABLE HiveToHBase_Business SELECT *;



