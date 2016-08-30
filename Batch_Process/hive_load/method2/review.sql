add jar /home/cloudera/project/week1/hive-hcatalog-core-0.13.0.jar;

--this is review table
DROP TABLE IF EXISTS test_review;
CREATE EXTERNAL TABLE test_review(
votes STRUCT<funny : BIGINT, useful : BIGINT, cool : BIGINT>,
user_id STRING,
review_id STRING,
stars FLOAT,
date STRING,
text STRING,
type STRING,
business_id STRING
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '/home/cloudera/project/week1/yelp_training_set_review.json' OVERWRITE INTO TABLE test_review;
