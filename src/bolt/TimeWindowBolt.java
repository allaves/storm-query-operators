package bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.coordination.IBatchBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/*
 * This bolt collects tuples from a spout until the time expires. 
 * The time window is implemented via tick tuples that the system sends every X seconds.
 * See http://www.michael-noll.com/blog/2013/01/18/implementing-real-time-trending-topics-in-storm/
 * Then, these tuples are emitted together to the following bolt.
 * 
 * QUESTION: Could we filter tuples by query predicates here?
 */
public class TimeWindowBolt extends BaseRichBolt {

	private static final long serialVersionUID = 57228724214461654L;
	
	private Fields fields;
	private int windowInterval;
	
	private OutputCollector collector;
	private List<Object> tuples;
	
	public TimeWindowBolt(Fields fields, Integer windowInterval) {
		this.fields = fields;
		this.windowInterval = windowInterval;
		tuples = new ArrayList<Object>();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(this.fields);
	}

	/*
	 * Defines the time window by configuring the sending of tick tuples each [windowInterval] seconds
	 */
	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, windowInterval);
		return conf;
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,	OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		if (isTickTuple(tuple)) {
			finishBatch();
		}
		else {
			tuples.add(tuple);
			collector.ack(tuple);
		}
	}

	/*
	 * Returns true if the argument is a tick tuple and false otherwise
	 */
	private boolean isTickTuple(Tuple tuple) {
		return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) &&
				tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
	}

	/*
	 * Emit the list of tuples when the window time expires
	 */
	public void finishBatch() {
		collector.emit(tuples);
	}

	

	

}
