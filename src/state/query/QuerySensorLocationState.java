package state.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import backtype.storm.tuple.Values;
import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;
import utils.LatLon;

/*
 * Query function for the sensor location state
 */
public class QuerySensorLocationState extends BaseQueryFunction<MemoryMapState<LatLon<String>>, LatLon<String>> {

	private static final long serialVersionUID = -5685780054277151569L;
	
	/*
	 * Returns a list with the locations corresponding to the sensor ids passed in the batch of tuples
	 * (non-Javadoc)
	 * @see storm.trident.state.QueryFunction#batchRetrieve(storm.trident.state.State, java.util.List)
	 */
	@Override
	public List<LatLon<String>> batchRetrieve(MemoryMapState<LatLon<String>> state, List<TridentTuple> tuples) {
		List<Object> sensorIds = new ArrayList<Object>();
		for (TridentTuple tuple : tuples) {
			sensorIds.add(tuple.getString(0));
		}
		return state.multiGet(Arrays.asList(sensorIds));
	}

	
	@Override
	public void execute(TridentTuple tuple, LatLon<String> location, TridentCollector collector) {
		if (location != null) {
			collector.emit(new Values(location.getLat(), location.getLon()));
		}
	}

}
