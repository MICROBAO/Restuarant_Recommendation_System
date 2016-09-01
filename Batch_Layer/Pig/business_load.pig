REGISTER '/home/cloudera/project/week3/piggybank-0.15.0.jar'; 
REGISTER '/home/cloudera/project/week3/elephant-bird-hadoop-compat-4.14-RC2.jar';
REGISTER '/home/cloudera/project/week3/elephant-bird-core-4.14-RC2.jar';
REGISTER '/home/cloudera/project/week3/elephant-bird-pig-4.14-RC2.jar';
REGISTER '/home/cloudera/project/week3/json-simple-1.1.1.jar';

--load json file into jsonloader
business_jsonLoader = LOAD 'user/cloudera/project/yelp_data/yelp_training_set_business.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS (business:map []);

--extract business_id, full_address, open, categories, city, review_count,name 
json_business_row = FOREACH business_jsonLoader GENERATE (chararray)business#'business_id' As business_id, (chararray)REPLACE(business#'full_address', '\\n', ', ') As full_address, (boolean)business#'open' As open, business#'categories' As categories, (chararray)business#'city' As city, (int)business#'review_count' As review_count, (chararray)business#'name' As name, (double)business#'longitude' As longitude, (chararray)business#'state' As state, (double)business#'stars' As stars, (double)business#'latitude' As latitude;

STORE json_business_row INTO 'hdfs:///user/cloudera/project/output/pig/business_table' USING PigStorage('\u0001');