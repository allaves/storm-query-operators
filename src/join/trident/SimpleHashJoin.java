package join.trident;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.tuple.Tuple;
import storm.trident.operation.Aggregator;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class SimpleHashJoin implements Aggregator<Map<String, TridentTuple>> {

	private static final long serialVersionUID = -8809389851369879551L;

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, TridentTuple> init(Object batchId, TridentCollector collector) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void aggregate(Map<String, TridentTuple> val, TridentTuple tuple, TridentCollector collector) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void complete(Map<String, TridentTuple> val,	TridentCollector collector) {
		// TODO Auto-generated method stub
		
	}

	

}
