package state.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

/*
 * Query function for the observations state
 */
public class QueryObservationsState extends BaseQueryFunction<MemoryMapState<Object>, Object> {

	private static final long serialVersionUID = 2488565232224517186L;

	@Override
	public List<Object> batchRetrieve(MemoryMapState<Object> state, List<TridentTuple> tuples) {
		List<Object> observationIds = new ArrayList<Object>();
		for (TridentTuple tuple : tuples) {
			observationIds.add(tuple.getString(0));
		}
		return state.multiGet(Arrays.asList(observationIds));
	}

	@Override
	public void execute(TridentTuple tuple, Object result, TridentCollector collector) {
		collector.emit(new Values(result));
	}

}
