package dongzhe.de.Spouts;


import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.Properties;
import java.io.IOException;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.FilterQuery;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.StallWarning;


import dongzhe.de.Utilities.*;

public class TwitterSpout extends BaseRichSpout {
	
	private static final long serialVersionUID = 1L;

	SpoutOutputCollector collect;
	LinkedBlockingQueue<Status> statusqueue = new LinkedBlockingQueue<Status>();
	TwitterStream twitterStream;
	@SuppressWarnings("rawtypes") 
	@Override
	public void open(Map conf, TopologyContext context,SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		collect=collector;
		
		//Twitter account authentication from properties file
		final Properties properties = new Properties();
		try {
			properties.load(TwitterSpout.class.getClassLoader()
					                .getResourceAsStream(Constants.CONFIG_PROPERTIES_FILE));
		} catch (final IOException exception) {
			//LOGGER.error(exception.toString());
			System.exit(1);
		}

		//Configuring Twitter OAuth
		final ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
		configurationBuilder.setIncludeEntitiesEnabled(true);
		//Get tokens
		configurationBuilder.setOAuthAccessToken(properties.getProperty(Constants.OAUTH_ACCESS_TOKEN));
		configurationBuilder.setOAuthAccessTokenSecret(properties.getProperty(Constants.OAUTH_ACCESS_TOKEN_SECRET));
		configurationBuilder.setOAuthConsumerKey(properties.getProperty(Constants.OAUTH_CONSUMER_KEY));
		configurationBuilder.setOAuthConsumerSecret(properties.getProperty(Constants.OAUTH_CONSUMER_SECRET));
		//Twitter Stream Declaration
		twitterStream = new TwitterStreamFactory(configurationBuilder.build()).getInstance();
		//Initialize a listener
		StatusListener listener = new StatusListener(){

			@Override
			public void onStatus(Status arg0) {
				statusqueue.offer(arg0);
			}
			//Irrelevant Functions
			@Override
			public void onException(Exception arg0) {}
			@Override
			public void onDeletionNotice(StatusDeletionNotice arg0) {}
			@Override
			public void onScrubGeo(long arg0, long arg1) {}
			@Override
			public void onStallWarning(StallWarning arg0) {}
			@Override
			public void onTrackLimitationNotice(int arg0) {}
			
		};
		
		twitterStream.addListener(listener);
		//only extract specific account
		FilterQuery tweetFilterQuery = new FilterQuery(); 
	    tweetFilterQuery.follow(new long[] { 	799247852L });
		twitterStream.filter(tweetFilterQuery);
	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		Status tempst = statusqueue.poll();
		if(tempst==null)
			Utils.sleep(50);
		else
			collect.emit(new Values(tempst));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("tweet"));
	}
}
