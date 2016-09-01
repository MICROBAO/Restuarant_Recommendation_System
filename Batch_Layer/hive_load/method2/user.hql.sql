add jar /home/cloudera/project/week1/hive-hcatalog-core-0.13.0.jar;

--this is the user table
DROP TABLE IF EXISTS test_user;
CREATE EXTERNAL TABLE test_user(
	votes STRUCT<funny : BIGINT, useful : BIGINT, cool : BIGINT>,
	user_id STRING,
	name STRING,
	average_stars FLOAT,
	review_count BIGINT,
	type STRING
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '/home/cloudera/project/week1/yelp_training_set_user.json' OVERWRITE INTO TABLE test_business;
