package spout;

import java.util.Map;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

public class RabbitMQSpout extends BaseRichSpout {

	private static final long serialVersionUID = -4386064122079596842L;

	private int timeWindowInSeconds;
	
	public RabbitMQSpout(int timeWindowInSeconds) {
		this.timeWindowInSeconds = timeWindowInSeconds;
	}
	
	@Override
	public void open(Map conf, TopologyContext context,	SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(""));
	}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, timeWindowInSeconds);
		return conf;
	}

}
