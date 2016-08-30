--Check_in

DROP TABLE IF EXISTS Checkin;

CREATE EXTERNAL TABLE Checkin(
value STRING)
COMMENT 'This is the Checkin data'
STORED AS TEXTFILE
LOCATION '/user/cloudera/project/yelp_data';

LOAD DATA INPATH 'user/cloudera/project/yelp_data/yelp_training_set_checkin.json' OVERWRITE INTO TABLE Checkin;


--LOAD DATA INPATH 'hdfs://quickstart.cloudera:8020/user/cloudera/user/cloudera/project/yelp_data/yelp_training_set_checkin.json' OVERWRITE INTO TABLE Checkin;
SELECT 
GET_JSON_OBJECT(Checkin.value,'$.checkin_info')
FROM Checkin LIMIT 10;