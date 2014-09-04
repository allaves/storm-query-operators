package topology;

import spout.RDFStreamSpout;
import spout.TupleTickerSpout;
import state.query.QueryEarthquakeObsState;
import state.update.EarthquakeObsUpdater;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import utils.GraphPrinter;
import utils.StreamPrinter;
import utils.Triple2Graph;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/*
 * Trident example for converting triples to timestamped graphs
 * Based on https://svn.apache.org/repos/asf/jena/trunk/jena-arq/src-examples/arq/examples/riot/ExRIOT_6.java
 */
public class Triple2GraphTopology {
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		
		String fileName = "data/Earthquakes-Spain-2013.ttl";
		
		// Emits RDF triples from a file
		// FROM -> The spout initialization should be defined in the FROM clause, as well as the window interval, e.g. [NOW - 1 HOURS]
		RDFStreamSpout rdfStreamSpout = new RDFStreamSpout(fileName);
		
		// Topology, state, and streams definition
		TridentTopology tridentTopology = new TridentTopology();
		
		// Windowed join with partitionPersist and MemoryMapState
		// Persisted trident state for earthquake observations (with MemoryMapState and partitionPersist)
		// Two options at this stage: a) assume we receive graphs and store them with an id and timestamp or b) assume we receive triples and a signal of graph beginning/termination.
		
		tridentTopology.newStream("earthquakeObs", rdfStreamSpout).
		each(new Fields("s", "p", "o"), new Triple2Graph(), new Fields("name", "timestamp", "graph")).
			each(new Fields("graph"), new GraphPrinter(), new Fields("graph2"));
		
//		TridentState earthquakeObsState = graphStream.partitionPersist(new MemoryMapState.Factory(), 
//				new Fields("name", "timestamp", "graph"), new EarthquakeObsUpdater(timeWindowInSeconds));
		
//		graphStream.stateQuery(earthquakeObsState, 
//				new Fields("timestamp"), new QueryEarthquakeObsState(timeWindowInSeconds), new Fields("graph"));
			
			//each(new Fields("graph"), new StreamPrinter(), new Fields("graph2"));
		
		
		
		Config conf = new Config();
		conf.setNumWorkers(3);			// Random number
	    conf.setDebug(true);
		
		LocalCluster cluster = new LocalCluster();
		
	    cluster.submitTopology("Triple2GraphTopology", conf, tridentTopology.build());
	    
	    Utils.sleep(100000);
	    cluster.shutdown();
	}
}
	


