

--beeline
beeline 
!connect jdbc:hive2://localhost:10000/default 
add jar /home/cloudera/project/week1/hive-hcatalog-core-0.13.0.jar;
DROP TABLE HiveToHBase_User;
DROP TABLE User;

--Create HBase table from Hive 

CREATE TABLE HiveToHBase_User(
    votes STRUCT<
                 useful:INT,  
                 funny:INT, 
                 cool:INT 
                >,
    user_id STRING,
    name STRING,
    average_stars DOUBLE,
    review_count INT,
    type STRING
    )
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = 'usercf:votes, :key, usercf:name, usercf:average_stars, usercf:review_count, usercf:type')
TBLPROPERTIES ('hbase.table.name' = 'Yelp_User_FromHive_Guan');



CREATE EXTERNAL TABLE User(
    votes STRUCT<
                 useful:INT,  
                 funny:INT, 
                 cool:INT 
                >,
    user_id STRING,
    name STRING,
    average_stars DOUBLE,
    review_count INT,
    type STRING
    )
COMMENT 'DATA ABOUT user on yelp'
ROW FORMAT SERDE 'com.cloudera.hive.serde.JSONSerDe'
LOCATION '/home/cloudera/project/week1';



5. Interacting with data (FROM source_hive_table INSERT INTO TABLE my_hbase_table)

FROM User INSERT INTO TABLE HiveToHBase_User SELECT *;





