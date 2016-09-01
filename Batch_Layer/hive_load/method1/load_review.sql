--Check_in

DROP TABLE IF EXISTS Review;

CREATE EXTERNAL TABLE Review(
value STRING)
COMMENT 'This is the Review data'
STORED AS TEXTFILE
LOCATION '/user/cloudera/project/yelp_data';

LOAD DATA INPATH 'user/cloudera/project/yelp_data/yelp_training_set_review.json' OVERWRITE INTO TABLE Review;

--LOAD DATA INPATH 'hdfs://quickstart.cloudera:8020/user/cloudera/user/cloudera/project/yelp_data/yelp_training_set_review.json' OVERWRITE INTO TABLE Review;
SELECT 
GET_JSON_OBJECT(Review.value,'$.review_id')
FROM Review LIMIT 10;