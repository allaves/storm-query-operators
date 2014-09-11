package topology.trident;

import java.util.HashMap;

import state.query.QueryObservationsState;
import state.query.QuerySensorLocationState;
import state.update.ObservationsUpdater;
import state.update.SensorLocationUpdater;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.map.MapState;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import utils.StreamPrinter;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/*
 * Trident example for windowed join 
 */
public class WindowedJoinTridentExampleLatLon {
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		
		// This spouts emits the same observations over and over again
		FixedBatchSpout observationSpout = new FixedBatchSpout(new Fields("obsId", "observedProperty", "value", "uom", "timestamp", "sensorId"), 1,
				new Values("obs1", "temperature", "35.0", "degrees Celsius", Long.toString(System.currentTimeMillis()), "sensor1"), 
				new Values("obs2", "temperature", "33.3", "degrees Celsius", Long.toString(System.currentTimeMillis()), "sensor2"), 
				new Values("obs3", "temperature", "34.1", "degrees Celsius", Long.toString(System.currentTimeMillis()), "sensor3"),
				new Values("obs4", "temperature", "35.2", "degrees Celsius", Long.toString(System.currentTimeMillis()), "sensor4"),
				new Values("obs5", "temperature", "31.4", "degrees Celsius", Long.toString(System.currentTimeMillis()), "sensor5"));
		observationSpout.setCycle(true);
		
		// This spouts emits the same sensor locations over and over again
		FixedBatchSpout sensorSpout = new FixedBatchSpout(new Fields("sensorId", "lat", "lon"), 1, 
				new Values("sensor1", "40.4055381", "-3.8399521"), 
				new Values("sensor2", "40.4055382", "-3.8399522"),
				new Values("sensor3", "40.4055383", "-3.8399523"),
				new Values("sensor4", "40.4055384", "-3.8399524"),
				new Values("sensor5", "40.4055385", "-3.8399525"),
				new Values("sensor6", "40.4055386", "-3.8399526"),
				new Values("sensor7", "40.4055387", "-3.8399527"));
		sensorSpout.setCycle(true);
		
		// Topology, state, and streams definition
		TridentTopology tridentTopology = new TridentTopology();
		
		// Windowed join with partitionPersist and MemoryMapState
		// Persisted trident state for sensor locations (with MemoryMapState and partitionPersist)
		TridentState sensorLocationsState = tridentTopology.newStream("sensorStream", sensorSpout).
				partitionPersist(new MemoryMapState.Factory(), 
						new Fields("sensorId", "lat", "lon"), 
						new SensorLocationUpdater());
		
		// Trident stream for observations that query the persisted state for sensor locations using stateQuery
		// Join results are shown as prints (PRINTED STREAM:...) 
		tridentTopology.newStream("observationStream", observationSpout).
				stateQuery(sensorLocationsState, new Fields("sensorId"), 
						new QuerySensorLocationState(), 
						new Fields("lat", "lon")).
						each(new Fields("obsId", "observedProperty", "value", "uom", "timestamp", "sensorId", "lat", "lon"), 
								new StreamPrinter(), 
								new Fields("obsId2", "observedProperty2", "value2", "uom2", "timestamp2", "lat2", "lon2"));
		
		Config conf = new Config();
		conf.setNumWorkers(3);			// Random number
	    conf.setDebug(true);
		
		LocalCluster cluster = new LocalCluster();
		
	    cluster.submitTopology("tridentJoinExample", conf, tridentTopology.build());
	    
	    Utils.sleep(10000);
	    cluster.shutdown();
	}
}
	


