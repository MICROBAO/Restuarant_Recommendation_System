package dongzhe.de.Bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.Status;

import java.util.Map;

public class DisplayBolt implements IRichBolt {
	private static final long serialVersionUID = 6108958246926484719L;
    private static final Logger logger = LoggerFactory.getLogger(DisplayBolt.class);
	OutputCollector _collector;
	@SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
    	//only get tweets contents
    	Status tweet = (Status) tuple.getValueByField("tweet");
    	String text = tweet.getText();
        _collector.emit(new Values(text));
        //logger.info("\n\n");
        logger.info(text);
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tweet"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}