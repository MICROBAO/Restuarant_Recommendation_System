
--beeline
beeline 
!connect jdbc:hive2://localhost:10000/default 
add jar /home/cloudera/project/week1/hive-hcatalog-core-0.13.0.jar;


DROP TABLE IF EXISTS Review;
DROP TABLE IF EXISTS HiveToHBase_Review;
--
DROP TABLE Yelp_Business;


--Create HBase table from Hive 

CREATE TABLE HiveToHBase_Review(
    votes STRUCT<
                 useful:INT,  
                 funny:INT, 
                 cool:INT 
                >,
    user_id STRING,
    review_id STRING,
    stars INT,
    `date` STRING,
    text STRING,
    type STRING,
    business_id STRING
    )
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = 'reviewcf:votes, reviewcf:user_id, :key, reviewcf:stars, reviewcf:`date`, reviewcf:text, reviewcf:type, reviewcf:business_id')
TBLPROPERTIES ('hbase.table.name' = 'HiveToHBase_Review');



CREATE EXTERNAL TABLE Review(
    votes STRUCT<
                 useful:INT,  
                 funny:INT, 
                 cool:INT 
                >,
    user_id STRING,
    review_id STRING,
    stars INT,
    `date` STRING,
    text STRING,
    type STRING,
    business_id STRING
)
COMMENT 'review data'
ROW FORMAT SERDE 'com.cloudera.hive.serde.JSONSerDe'
LOCATION '/home/cloudera/project/week1';



-Interacting with data (FROM source_hive_table INSERT INTO TABLE my_hbase_table)

FROM Review INSERT INTO TABLE HiveToHBase_Review SELECT *;




