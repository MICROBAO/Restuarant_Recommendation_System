
--load yelp data to hive



--load business table
--this method use the built-in function GET_JSON_OBJECT
--Extracts json object from a json string based on json path specified, 
--and returns json string of the extracted json object. It will return 
--null if the input json string is invalid. NOTE: The json path can only 
--have the characters [0-9a-z_], i.e., no upper-case or special 
--characters. Also, the keys *cannot start with numbers.* This is due to
--restrictions on Hive column names.

DROP TABLE IF EXISTS Business;

CREATE EXTERNAL TABLE Business(
value STRING)
COMMENT 'This is the busniness data'
STORED AS TEXTFILE
LOCATION '/user/cloudera/project/yelp_data';

LOAD DATA INPATH 'user/cloudera/project/yelp_data/yelp_training_set_business.json' OVERWRITE INTO TABLE Business;
--if using beeline than input path must be absolute path in hdfs
--LOAD DATA INPATH 'hdfs://quickstart.cloudera:8020/user/cloudera/user/cloudera/project/yelp_data/yelp_training_set_business.json' OVERWRITE INTO TABLE Business;

--select the corresponding title
SELECT 
GET_JSON_OBJECT(Business.value,'$.stars'), 
GET_JSON_OBJECT(Business.value,'$.name'),
GET_JSON_OBJECT(Business.value,'$.city'),
GET_JSON_OBJECT(Business.value,'$.review_count')
FROM business LIMIT 10;




