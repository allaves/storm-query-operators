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
	
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		int timePeriodInSeconds = 10;
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, timePeriodInSeconds);
		return conf;
	}

}
