package topology;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.lang.PipedRDFIterator;
import org.apache.jena.riot.lang.PipedRDFStream;
import org.apache.jena.riot.lang.PipedTriplesStream;
import org.apache.zookeeper.proto.op_result_t;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

import spout.RDFStreamSpout;
import state.query.QuerySensorLocationState;
import state.update.SensorLocationUpdater;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import utils.StreamPrinter;
import utils.TripleParser;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/*
 * Trident example for joining RDF streams
 * Based on https://svn.apache.org/repos/asf/jena/trunk/jena-arq/src-examples/arq/examples/riot/ExRIOT_6.java
 */
public class RDFJoinTridentExample {
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		
		String fileName = "/data/Earthquakes-Spain-2013.ttl";
		int timeWindowInSeconds = 30;
		
		// Emits RDF triples from a file
		RDFStreamSpout rdfStreamSpout = new RDFStreamSpout(fileName, timeWindowInSeconds);
		
		// Topology, state, and streams definition
		TridentTopology tridentTopology = new TridentTopology();
		
		// Windowed join with partitionPersist and MemoryMapState
		// Persisted trident state for sensor locations (with MemoryMapState and partitionPersist)
//		TridentState sensorLocationsState = tridentTopology.newStream("sensorStream", sensorSpout).
//				partitionPersist(new MemoryMapState.Factory(), 
//						new Fields("sensorId", "lat", "lon"), 
//						new SensorLocationUpdater());
		
		// Trident stream for observations that query the persisted state for sensor locations using stateQuery
		// Join results are shown as prints (PRINTED STREAM:...) 
		tridentTopology.newStream("earthquakeObservations", rdfStreamSpout).
			each(new Fields("triples"), new StreamPrinter(), new Fields("triples2"));
		
		//each(new Fields("triples"), new TripleParser(), new Fields("subject", "predicate", "object")).
		
		
		
//				stateQuery(sensorLocationsState, new Fields("sensorId"), 
//						new QuerySensorLocationState(), 
//						new Fields("lat", "lon")).
//						each(new Fields("obsId", "observedProperty", "value", "uom", "timestamp", "sensorId", "lat", "lon"), 
//								new StreamPrinter(), 
//								new Fields("obsId2", "observedProperty2", "value2", "uom2", "timestamp2", "lat2", "lon2"));
		
		Config conf = new Config();
		conf.setNumWorkers(3);			// Random number
	    conf.setDebug(true);
		
		LocalCluster cluster = new LocalCluster();
		
	    cluster.submitTopology("RDFStreamJoinExample", conf, tridentTopology.build());
	    
	    Utils.sleep(10000);
	    cluster.shutdown();
	}
}
	


