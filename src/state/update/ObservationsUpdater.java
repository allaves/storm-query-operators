package state.update;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

/*
 * Updates the state of the observations
 * Fields: "obsId", "observedProperty", "value", "uom", "timestamp", "sensorId"
 */
public class ObservationsUpdater extends BaseStateUpdater<MemoryMapState<Object>> {

	private static final long serialVersionUID = 9153312906840806195L;

	/*
	 * (non-Javadoc)
	 * @see storm.trident.state.StateUpdater#updateState(storm.trident.state.State, java.util.List, storm.trident.operation.TridentCollector)
	 */
	@Override
	public void updateState(MemoryMapState<Object> state, List<TridentTuple> tuples, TridentCollector collector) {
		List<Object> observationIds = new ArrayList<Object>();
		HashMap<String, String> aux = new HashMap<String, String>();
		List<Object> values = new ArrayList<Object>();
		for (TridentTuple tuple : tuples) {
			observationIds.add(tuple.getStringByField("obsId"));
			aux.put("observedProperty", tuple.getStringByField("observedProperty"));
			aux.put("value", tuple.getStringByField("value"));
			aux.put("uom", tuple.getStringByField("uom"));
			aux.put("timestamp", tuple.getStringByField("timestamp"));
			aux.put("sensorId", tuple.getStringByField("sensorId"));
			values.add(aux);
			aux.clear();
		}
		state.multiPut(Arrays.asList(observationIds), values);
	}

}
