package bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.coordination.IBatchBolt;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/*
 * This bolt collects tuples from a spout until the time expires.
 * Then, these tuples are emitted together to the following bolt.
 * 
 * QUESTION: Could we filter tuples by query predicates here?
 */
public class TimeWindowBolt implements IBatchBolt {

	private static final long serialVersionUID = 57228724214461654L;
	
	private Fields fields;
	private int timeout;
	
	private BatchOutputCollector collector;
	private List<Tuple> tuples;
	
	public TimeWindowBolt(Fields fields, Integer timeout) {
		this.fields = fields;
		this.timeout = timeout;
		tuples = new ArrayList<Tuple>();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(this.fields);
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		// TODO: When the first tuple of the batch is received, a countdown starts until timeout expires
		tuples.add(tuple);
	}

	@Override
	public void finishBatch() {
		collector.emit(tuples);
		collector.ack(tuples);
	}

	

}
