package dongzhe.de.Bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;


public class WordCountBolt extends BaseRichBolt {
	private static final long serialVersionUID = 2706047697068872387L;
	private static final Logger logger = LoggerFactory.getLogger(WordCountBolt.class);

    private final long _logIntervalSec;
    private final long _clearIntervalSec;
    private final int _topListSize;

    private Map<String, Long> counter;
    private long lastLogTime;
    private long lastClearTime;

    public WordCountBolt(long logIntervalSec, long clearIntervalSec, int topListSize) {
        _logIntervalSec = logIntervalSec;
        _clearIntervalSec = clearIntervalSec;
        _topListSize = topListSize;
    }
	@SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        counter = new HashMap<String, Long>();
        lastLogTime = System.currentTimeMillis();
        lastClearTime = System.currentTimeMillis();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    	outputFieldsDeclarer.declare(new Fields("wordCount"));
    }

    @Override
    public void execute(Tuple input) {
        String word = (String) input.getValueByField("word");
        Long count = counter.get(word);
        count = count == null ? 1L : count + 1;
        counter.put(word, count);

        long current = System.currentTimeMillis();
        long logPeriodSec = (current - lastLogTime) / 1000;
        if (logPeriodSec > _logIntervalSec) {
        	logger.info("\n\n");
        	logger.info("Word count: "+counter.size());

            publishTopList();
            lastLogTime = current;
        }
    }

    private void publishTopList() {
        // calculate top list:
        SortedMap<Long, String> top = new TreeMap<Long, String>();
        for (Map.Entry<String, Long> entry : counter.entrySet()) {
            long count = entry.getValue();
            String word = entry.getKey();

            top.put(count, word);
            if (top.size() > _topListSize) {
                top.remove(top.firstKey());
            }
        }

        for (Map.Entry<Long, String> entry : top.entrySet()) {
            logger.info(new StringBuilder("top: ").append(entry.getValue()).append('|').append(entry.getKey()).toString());
        }

        // Clear top list
        long now = System.currentTimeMillis();
        if (now - lastClearTime > _clearIntervalSec * 1000) {
            counter.clear();
            lastClearTime = now;
        }
    }
}