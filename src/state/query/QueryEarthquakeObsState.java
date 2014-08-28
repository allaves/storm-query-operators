package state.query;

import java.util.List;

import backtype.storm.Constants;
import backtype.storm.tuple.Tuple;

import com.hp.hpl.jena.graph.Graph;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

public class QueryEarthquakeObsState extends BaseQueryFunction<MemoryMapState<Graph>, Graph> {

	private static final long serialVersionUID = -3217320295548857585L;

	@Override
	public List<Graph> batchRetrieve(MemoryMapState<Graph> state,
			List<TridentTuple> args) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void execute(TridentTuple tuple, Graph result, TridentCollector collector) {
		if (isTickTuple(tuple)) {
			// Query
			
		}
	}

	/*
	 * Returns true if the argument is a tick tuple and false otherwise
	 */
	private boolean isTickTuple(Tuple tuple) {
		return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) &&
				tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
	}
	}

}
