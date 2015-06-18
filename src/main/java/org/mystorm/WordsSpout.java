package org.mystorm;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class WordsSpout implements IRichSpout {

	SpoutOutputCollector _collector;
	
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
		
	}

	public void close() {
		// TODO Auto-generated method stub
		
	}

	public void activate() {
		// TODO Auto-generated method stub
		
	}

	public void deactivate() {
		// TODO Auto-generated method stub
		
	}
	public void nextTuple() {
		Utils.sleep(100);
		final String[] words = new String[] {"nathan", "mike", "jackson", "golda", "bertels"};
	    final Random rand = new Random();
	    final String word = words[rand.nextInt(words.length)];
	    _collector.emit(new Values(word));
		
	}

	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		
	}

	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
		
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	
}