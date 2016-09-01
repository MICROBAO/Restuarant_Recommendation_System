--load User table

DROP TABLE IF EXISTS User;

CREATE EXTERNAL TABLE User(
value STRING)
COMMENT 'This is the User data'
STORED AS TEXTFILE
LOCATION '/user/cloudera/project/yelp_data';

LOAD DATA INPATH 'user/cloudera/project/yelp_data/yelp_training_set_user.json' OVERWRITE INTO TABLE User;
--LOAD DATA INPATH 'hdfs://quickstart.cloudera:8020/user/cloudera/user/cloudera/project/yelp_data/yelp_training_set_user.json' OVERWRITE INTO TABLE User;

SELECT 
GET_JSON_OBJECT(User.value,'$.name')
FROM User LIMIT 10;