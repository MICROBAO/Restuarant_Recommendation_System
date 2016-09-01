package dongzhe.de.Topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import backtype.storm.spout.SchemeAsMultiScheme;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;
import java.util.UUID;

import dongzhe.de.Bolts.*;
import dongzhe.de.Spouts.*;
import dongzhe.de.Utilities.*;




public class TwitterTopology {
	//logger file to load topology
	private static final Logger LOGGER = LoggerFactory.getLogger(TwitterTopology.class);
	public static final void main(final String[] args) {
		try {
			Properties topologyConfig  = new Properties();
			final Config config = new Config();
			config.setMessageTimeoutSecs(20);
			
			//build topology that consist one spout from twitter api
			//three bolt: DisplayBolt to show every new tweet; WordSplitBolt split the sentence; WordCounterBolt count the words
			//spout emits tweets to DisplayBolt and WordSplitBolt

			//topologyBuilder.setSpout("TwitterSpout", new TwitterSpout(),1);
			//topologyBuilder.setBolt("DisplayBolt", new DisplayBolt()).shuffleGrouping("TwitterSpout");
			//topologyBuilder.setBolt("WordSplitBolt", new WordSplitBolt(4)).shuffleGrouping("TwitterSpout");
			

			String configFileLocation = "configuration.properties";
		    
			topologyConfig.load(ClassLoader.getSystemResourceAsStream(configFileLocation));
			
			//zookeeper=localhost:2181
			//topic=tweetsnew1
			String zkConnString = topologyConfig.getProperty("zookeeper");
			String topicName = topologyConfig.getProperty("topic");
			
			BrokerHosts zkhost = new ZkHosts(zkConnString);
			SpoutConfig spoutConfig = new SpoutConfig(zkhost, topicName, "/" + topicName, UUID.randomUUID().toString());
			spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
			KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);			
			
			//set kafka as spout 
			TopologyBuilder topologyBuilder = new TopologyBuilder();
			topologyBuilder.setSpout("kafkaTweetSpout", kafkaSpout, 1);
			topologyBuilder.setBolt("tweetOriginal", new TweetKafkabolt(),1).shuffleGrouping("kafkaTweetSpout");
			topologyBuilder.setBolt("WordSplitBolt", new WordSplitBolt(5)).shuffleGrouping("tweetOriginal");
			topologyBuilder.setBolt("WordCountBolt", new WordCountBolt(10, 5 * 60, 50)).shuffleGrouping("WordSplitBolt");

			//cluster mode
			if (args != null && args.length > 0) {
				config.setNumWorkers(3);
				StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
			} else {
				//local mode
				config.setMaxTaskParallelism(10);
				final LocalCluster localCluster = new LocalCluster();
				localCluster.submitTopology(Constants.TOPOLOGY_NAME, config, topologyBuilder.createTopology());
				//run 100 secs
				Utils.sleep(300 * 1000);
				LOGGER.info("Shutting down the local cluster");
				localCluster.killTopology(Constants.TOPOLOGY_NAME);
				localCluster.shutdown();
			}
		} catch (final InvalidTopologyException exception) {
			exception.printStackTrace();
		} catch (final Exception exception) {
			exception.printStackTrace();
		}
	}
}