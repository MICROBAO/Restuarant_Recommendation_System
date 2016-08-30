add jar /home/cloudera/project/week1/hive-hcatalog-core-0.13.0.jar;

--use external table to maintain the file while drop the table.
--THIS IS THE TABLE FOR BUSINESS

DROP TABLE IF EXISTS test_business;
CREATE EXTERNAL TABLE test_business(
business_id STRING,
full_address STRING,
open BOOLEAN,
categories ARRAY<STRING> comment 'localized category names',
city STRING,
review_count BIGINT,
name STRING,
neighborhoods ARRAY<STRING> comment 'neighborhoods name',
longitude STRING,
state STRING,
stars FLOAT,
latitude STRING,
type STRING
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION
'/home/cloudera/project/week1';
LOAD DATA LOCAL INPATH '/home/cloudera/project/week1/yelp_training_set_business.json' OVERWRITE INTO TABLE test_business;
--LOAD DATA INPATH  'hdfs://quickstart.cloudera:8020/user/cloudera/user/cloudera/project/yelp_data/yelp_training_set_business.json' OVERWRITE INTO TABLE test_business;
--test
