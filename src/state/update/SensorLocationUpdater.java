package state.update;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;
import utils.LatLon;

/*
 * Updates the state with the location of the sensors
 */
public class SensorLocationUpdater extends BaseStateUpdater<MemoryMapState<Object>> {

	private static final long serialVersionUID = -3382077897620826620L;

	/*
	 * (non-Javadoc)
	 * @see storm.trident.state.StateUpdater#updateState(storm.trident.state.State, java.util.List, 
	 * storm.trident.operation.TridentCollector)
	 */
	@Override
	public void updateState(MemoryMapState<Object> state, List<TridentTuple> tuples, TridentCollector collector) {
		List<Object> sensorIds = new ArrayList<Object>();
		List<Object> locations = new ArrayList<Object>();
		for (TridentTuple tuple : tuples) {
			sensorIds.add(tuple.getString(0));
			// Works for lat lon as separate fields
			locations.add(new LatLon<String>(tuple.getString(1), tuple.getString(2)));
		}
		state.multiPut(Arrays.asList(sensorIds), locations);
		// Tuples emitted to the collector go through the newValuesStream call
		//collector.emit(state.multiGet(Arrays.asList(sensorIds)));
	}

}
