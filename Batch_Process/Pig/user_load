REGISTER '/home/cloudera/project/week3/piggybank-0.15.0.jar'; 
REGISTER '/home/cloudera/project/week3/elephant-bird-hadoop-compat-4.14-RC2.jar';
REGISTER '/home/cloudera/project/week3/elephant-bird-core-4.14-RC2.jar';
REGISTER '/home/cloudera/project/week3/elephant-bird-pig-4.14-RC2.jar';
REGISTER '/home/cloudera/project/week3/json-simple-1.1.1.jar';

user_jsonLoader = LOAD 'user/cloudera/project/yelp_data/yelp_training_set_user.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS (user:map []);

json_user = FOREACH user_jsonLoader GENERATE (map[])user#'checkin_info' As checkin_info, (chararray)user#'type' As type, (chararray)user#'business_id' As business_id;

STORE json_user INTO 'user/cloudera/project/output/pig/user_table' USING PigStorage('\u0001');