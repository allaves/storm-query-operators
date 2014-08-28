package state.update;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.state.BaseStateUpdater;
import storm.trident.state.State;
import storm.trident.state.StateUpdater;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;
import utils.LatLon;

/*
 * Updates the state with the last earthquake observations
 */
public class EarthquakeObsUpdater extends BaseStateUpdater<MemoryMapState<Object>> {

	private static final long serialVersionUID = 964509701435526966L;

	@Override
	public void updateState(MemoryMapState<Object> state, List<TridentTuple> tuples, TridentCollector collector) {
		List<Object> names = new ArrayList<Object>();
		//List<Object> timestamps = new ArrayList<Object>();
		List<Object> earthquakeGraphs = new ArrayList<Object>();
		for (TridentTuple tuple : tuples) {
			names.add(tuple.get(0));
			earthquakeGraphs.add(tuple.get(1));
		}
		state.multiPut(Arrays.asList(names), earthquakeGraphs);
		
		// Tuples emitted to the collector go through the newValuesStream call
		//collector.emit(state.multiGet(Arrays.asList(sensorIds)));
	}

	

}
