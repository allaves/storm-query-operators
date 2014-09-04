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
	
	private int timeWindowInSeconds;
	
	public EarthquakeObsUpdater(int timeWindowInSeconds) {
		this.timeWindowInSeconds = timeWindowInSeconds;
	}

	/*
	 * Takes the first tuple of the batch and checks the timestamp t0.
	 * It stores tuples into a state while the timestamp is within [t0 AND t0+timeWindowInSeconds]
	 * When the timestamp tn of a tuple does not match this condition, THEN t0 = tn, and the previous state is overwritten with new tuples.
	 * (non-Javadoc)
	 * @see storm.trident.state.StateUpdater#updateState(storm.trident.state.State, java.util.List, storm.trident.operation.TridentCollector)
	 */
	@Override
	public void updateState(MemoryMapState<Object> state, List<TridentTuple> tuples, TridentCollector collector) {
		List<Object> ids = new ArrayList<Object>();
		List<Object> earthquakeGraphs = new ArrayList<Object>();
		for (TridentTuple tuple : tuples) {
			ids.add((String)tuple.get(0) + (String)tuple.get(1));
			earthquakeGraphs.add(tuple.get(2));
		}
		state.multiPut(Arrays.asList(ids), earthquakeGraphs);
		
		// Tuples emitted to the collector go through the newValuesStream call
		//collector.emit(state.multiGet(Arrays.asList(sensorIds)));
	}

	

}
