package state.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import backtype.storm.Constants;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.hp.hpl.jena.graph.Graph;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

public class QueryEarthquakeObsState extends BaseQueryFunction<MemoryMapState<Graph>, Graph> {

	private static final long serialVersionUID = -3217320295548857585L;

	private int timeWindowInSeconds;
	private long currentTimestamp;
	
	public QueryEarthquakeObsState(int timeWindowInSeconds) {
		this.timeWindowInSeconds = timeWindowInSeconds;
		this.currentTimestamp = System.currentTimeMillis();
	}
	
	@Override
	public List<Graph> batchRetrieve(MemoryMapState<Graph> state, List<TridentTuple> tuples) {
		List<String> ids = new ArrayList<String>();
		for (TridentTuple tuple : tuples) {
			ids.add(tuple.getString(0));
		}
		return state.multiGet(Arrays.asList(graphs));
	}

	@Override
	public void execute(TridentTuple tuple, Graph result, TridentCollector collector) {
		if (result != null) {
			collector.emit(new Values(result));
			System.out.println("GRAPH EMITTED!");
		}
	}

}
