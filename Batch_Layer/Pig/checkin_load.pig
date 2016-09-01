REGISTER '/home/cloudera/project/week3/piggybank-0.15.0.jar'; 
REGISTER '/home/cloudera/project/week3/elephant-bird-hadoop-compat-4.14-RC2.jar';
REGISTER '/home/cloudera/project/week3/elephant-bird-core-4.14-RC2.jar';
REGISTER '/home/cloudera/project/week3/elephant-bird-pig-4.14-RC2.jar';
REGISTER '/home/cloudera/project/week3/json-simple-1.1.1.jar';

checkin_jsonLoader = LOAD 'user/cloudera/project/yelp_data/yelp_training_set_chenkin.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS (checkin:map []);

json_checkin = FOREACH json_checkin_row_jsonLoader GENERATE (map[])checkin#'checkin_info' As checkin_info, (chararray)checkin#'type' As type, (chararray)checkin#'business_id' As business_id;

STORE json_checkin INTO 'user/cloudera/project/output/pig/checkin_table' USING PigStorage('\u0001');