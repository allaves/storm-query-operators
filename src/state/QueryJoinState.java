package state;

import java.util.ArrayList;
import java.util.List;

import backtype.storm.tuple.Values;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

public class QueryJoinState extends BaseQueryFunction<MemoryMapState<TridentTuple>, List<Object>> {

	private static final long serialVersionUID = -5685780054277151569L;
	
	private String joinAttribute;
	
	public QueryJoinState(String joinAttribute) {
		this.joinAttribute = joinAttribute;
	}
	
	@Override
	public List<List<Object>> batchRetrieve(MemoryMapState<TridentTuple> state, List<TridentTuple> input) {
		List<List<Object>> keys = new ArrayList<List<Object>>();
		for (TridentTuple tuple : input) {
			keys.add(tuple.getStringByField(joinAttribute));
		}
		return state.multiGet(keys);
	}
	

	@Override
	public void execute(TridentTuple tuple, TridentTuple result, TridentCollector collector) {
		// TODO Auto-generated method stub
		
	}

	

}
