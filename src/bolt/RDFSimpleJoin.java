package bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class RDFSimpleJoin extends BaseRichBolt {

	private static final long serialVersionUID = -6606787987861179871L;
	
	private String joinAttribute;
	
	public RDFSimpleJoin(String joinAttribute) {
		this.joinAttribute = joinAttribute;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,	OutputCollector collector) {
		// TODO Auto-generated method stub

	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
