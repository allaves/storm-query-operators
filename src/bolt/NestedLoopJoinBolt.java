package bolt;

import java.util.Map;

import join.NestedLoopJoinOperator;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class NestedLoopJoinBolt implements IRichBolt {

	private static final long serialVersionUID = -4826491272348676855L;
	
	private OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context,	OutputCollector collector) {
		this.collector = collector;	
	}

	@Override
	public void execute(Tuple input) {
		
		
		NestedLoopJoinOperator.execute(s, r, joinAttribute, conditionS, conditionR);
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
