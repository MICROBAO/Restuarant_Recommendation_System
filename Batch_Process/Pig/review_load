REGISTER '/home/cloudera/project/week3/piggybank-0.15.0.jar'; 
REGISTER '/home/cloudera/project/week3/elephant-bird-hadoop-compat-4.14-RC2.jar';
REGISTER '/home/cloudera/project/week3/elephant-bird-core-4.14-RC2.jar';
REGISTER '/home/cloudera/project/week3/elephant-bird-pig-4.14-RC2.jar';
REGISTER '/home/cloudera/project/week3/json-simple-1.1.1.jar';

review_jasonLoader = LOAD 'user/cloudera/project/yelp_data/yelp_training_set_review.json' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS (review:map []);


json_review = FOREACH review_jsonLoader GENERATE (map[])review#'votes' As votes, (chararray)review#'user_id' As user_id, (chararray)review#'review_id' As review_id, (double)review#'stars' As stars, (chararray)review#'date' As date, (chararray)review#'text' As text;

STORE json_review INTO 'user/cloudera/project/output/pig/review_table' USING PigStorage('\u0001');