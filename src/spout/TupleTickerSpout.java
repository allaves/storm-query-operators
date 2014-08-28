package spout;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/*
 * This spout emit one tuple every
 */
public class TupleTickerSpout extends BaseRichSpout {

	private static final long serialVersionUID = 4564831968526472435L;
	
	private int windowTimeInSeconds;
	private SpoutOutputCollector collector;
	
	public TupleTickerSpout(int windowTimeInSeconds) {
		this.windowTimeInSeconds = windowTimeInSeconds;
	}

	@Override
	public void open(Map conf, TopologyContext context,	SpoutOutputCollector collector) {
		this.collector = collector;

	}

	@Override
	public void nextTuple() {
		Utils.sleep(windowTimeInSeconds*1000);
		collector.emit(new Values("TICK_TUPLE"));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tupleType"));
	}

}
