package topology;

import spout.RDFStreamSpout;
import spout.TupleTickerSpout;
import state.query.QueryEarthquakeObsState;
import state.update.EarthquakeObsUpdater;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.testing.MemoryMapState;
import utils.StreamPrinter;
import utils.Triple2Graph;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

/*
 * Trident example for joining RDF streams
 * Based on https://svn.apache.org/repos/asf/jena/trunk/jena-arq/src-examples/arq/examples/riot/ExRIOT_6.java
 */
public class RDFJoinTridentExample {
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		
		String fileName = "data/Earthquakes-Spain-2013.ttl";
		int timeWindowInSeconds = 2;
		
		// Emits RDF triples from a file
		// FROM -> The spout initialization should be defined in the FROM clause, as well as the window interval, e.g. [NOW - 1 HOURS]
		RDFStreamSpout rdfStreamSpout = new RDFStreamSpout(fileName);
		
		// Topology, state, and streams definition
		TridentTopology tridentTopology = new TridentTopology();
		
		// Windowed join with partitionPersist and MemoryMapState
		// Persisted trident state for earthquake observations (with MemoryMapState and partitionPersist)
		// Two options at this stage: a) assume we receive graphs and store them with an id and timestamp or b) assume we receive triples and a signal of graph beginning/termination.
		TridentState earthquakeObsState = tridentTopology.newStream("earthquakeObs", rdfStreamSpout).
				each(new Fields("s", "p", "o"), new Triple2Graph(), new Fields("id", "timestamp", "graph")).
				each(new Fields("id", "timestamp", "graph"), new StreamPrinter(), new Fields("id2", "timestamp2", "graph2")).
				partitionPersist(new MemoryMapState.Factory(), new Fields("id", "graph"), new EarthquakeObsUpdater());
		
		// Trident stream for earthquake observations that query the persisted state using stateQuery
//		tridentTopology.newStream("earthquakeObs", rdfStreamSpout).
		
		
		tridentTopology.newStream("timeWindow", new TupleTickerSpout(timeWindowInSeconds));
		
//		stateQuery(earthquakeObsState, new Fields("name"), 
//				new QueryEarthquakeObsState(), 
//				new Fields("graph")).
//				each(new Fields("obsId", "observedProperty", "value", "uom", "timestamp", "sensorId", "lat", "lon"), 
//						new StreamPrinter(), 
//						new Fields("obsId2", "observedProperty2", "value2", "uom2", "timestamp2", "lat2", "lon2"));
		
		// SELECT -> The last bolt will emit tuples with fields "observation" and "values"
		//each(new Fields("..."), new SelectOp(), new Fields("observation", "values"));
		
		
		Config conf = new Config();
		conf.setNumWorkers(3);			// Random number
	    conf.setDebug(true);
		
		LocalCluster cluster = new LocalCluster();
		
	    cluster.submitTopology("RDFStreamJoinExample", conf, tridentTopology.build());
	    
	    Utils.sleep(100000);
	    cluster.shutdown();
	}
}
	


