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
 * Updater function for sensor locations ("lat lon") in one field
 */
public class SimpleSensorLocationUpdater extends BaseStateUpdater<MemoryMapState<Object>> {
	
	private static final long serialVersionUID = -4987743172855188792L;

	@Override
	public void updateState(MemoryMapState<Object> state, List<TridentTuple> tuples, TridentCollector collector) {
		List<Object> sensorIds = new ArrayList<Object>();
		List<Object> locations = new ArrayList<Object>();
		for (TridentTuple tuple : tuples) {
			sensorIds.add(tuple.getString(0));
			locations.add(tuple.getString(1));
		}
		state.multiPut(Arrays.asList(sensorIds), locations);
		// Tuples emitted to the collector go through the newValuesStream call
		//collector.emit(state.multiGet(Arrays.asList(sensorIds)));
	}

	

}
