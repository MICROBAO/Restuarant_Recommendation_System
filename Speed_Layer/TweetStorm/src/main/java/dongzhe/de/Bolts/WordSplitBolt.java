package dongzhe.de.Bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import twitter4j.Status;

import java.util.Map;

public class WordSplitBolt extends BaseRichBolt {
	private static final long serialVersionUID = 5151173513759399636L;
	private final int minLength;

    private OutputCollector _collector;
    // the minium length of word is 4
    public WordSplitBolt(int minWordLength) {
        this.minLength = minWordLength;
    }
	@SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        _collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        //Status tweet = (Status) input.getValueByField("tweet");
        String tweet = (String) input.getValueByField("tweet");
        String text = tweet.replaceAll("\\p{Punct}", " ").replaceAll("\\r|\\n", "").toLowerCase();
        //String lang = tweet.getLang();
        //String text = tweet.getText().replaceAll("\\p{Punct}", " ").replaceAll("\\r|\\n", "").toLowerCase();
        String[] words = text.split(" ");
        for (String word : words) {
            if (word.length() >= minLength) {
                _collector.emit(new Values(word));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}