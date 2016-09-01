import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

public class TweetsProducer {
	
	public TweetListener twListener;
	
	class TweetListener implements StatusListener {

		KafkaProducer producer;
		LinkedBlockingQueue<Status> statusqueue = new LinkedBlockingQueue<Status>();
		
		public TweetListener() {
			Properties props = new Properties();
			props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
			props.setProperty(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG, Integer.toString(5 * 1000));
			props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
			props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
			producer = new KafkaProducer(props);
		}

		public void onStatus(Status status) {
			try {
				if (status.getLang().equals("en") && !status.isRetweet()) {
					statusqueue.offer(status);
					Status tempst = statusqueue.poll();
			    	String tweet = "n/a";

		       	   tweet = status.getText();
		       	   @SuppressWarnings("rawtypes")
		       	   ProducerRecord<String, Status> data = new ProducerRecord("tweets", tweet);
				   producer.send(data);

				}
			} catch (Exception e) {
				System.out.println("wrong");
			}
		}

		public void onDeletionNotice(StatusDeletionNotice sdn) {
		}

		public void onTrackLimitationNotice(int i) {
		}

		public void onScrubGeo(long l, long l1) {
		}

		public void onStallWarning(StallWarning warning) {
		}

		public void onException(Exception e) {
			e.printStackTrace();
		}

		
	}

	public TweetsProducer(){
		this.twListener = new TweetListener();
	}
	

	public static void main(String[] args) {

		TwitterStream twitterStream;
		ConfigurationBuilder config = ConfigurationProvider.getConfig();

		TwitterStreamFactory fact = new TwitterStreamFactory(config.build());
		twitterStream = fact.getInstance();

		TweetsProducer tp = new TweetsProducer();
		twitterStream.addListener(tp.twListener);

	    FilterQuery tweetFilterQuery = new FilterQuery(); // See 

	    tweetFilterQuery.follow(new long[] { 739682825863995393L });
		twitterStream.filter(tweetFilterQuery);
		//twitterStream.sample();
	}
}
