add jar /home/cloudera/project/week1/hive-hcatalog-core-0.13.0.jar;


--THIS IS TABLE FOR CHECKIN INFO
DROP TABLE IF EXISTS test_checkin;
CREATE EXTERNAL TABLE test_checkin(
checkin_info MAP<STRING, BITGNT>,
type STRING,
business_id STRING
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '/home/cloudera/project/week1/yelp_training_set_checkin.json' OVERWRITE INTO TABLE test_checkin;

