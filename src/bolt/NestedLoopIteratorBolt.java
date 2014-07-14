package bolt;

import java.util.ArrayList;
import java.util.Map;

import join.NestedLoopJoinOperator;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class NestedLoopIteratorBolt implements IRichBolt {

	private static final long serialVersionUID = -4826491272348676855L;
	
	private OutputCollector collector;
	private ArrayList<String> streamNames;
	private ArrayList<Integer> windowTimes;		// In seconds


	@Override
	public void prepare(Map stormConf, TopologyContext context,	OutputCollector collector) {
		this.collector = collector;	
		// Assign the name of the streams
		streamNames.add("s");
		streamNames.add("r");
		// Assign the temporal extent of the windows for each stream
		windowTimes.add(30);
		windowTimes.add(60);
	}

	@Override
	public void execute(Tuple input) {
		
		//NestedLoopJoinOperator.execute(s, r, joinAttribute, conditionS, conditionR);
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
