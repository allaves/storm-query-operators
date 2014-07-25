package state.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import backtype.storm.tuple.Values;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

public class SimpleQuerySensorLocationState extends BaseQueryFunction<MemoryMapState<String>, String> {

	private static final long serialVersionUID = 5499722124591618145L;

	@Override
	public List<String> batchRetrieve(MemoryMapState<String> state,	List<TridentTuple> tuples) {
		List<Object> sensorIds = new ArrayList<Object>();
		for (TridentTuple tuple : tuples) {
			sensorIds.add(tuple.getString(0));
		}
		return state.multiGet(Arrays.asList(sensorIds));
	}

	@Override
	public void execute(TridentTuple tuple, String location, TridentCollector collector) {
		if (location != null) {
			collector.emit(new Values(location));
		}
	}

}
