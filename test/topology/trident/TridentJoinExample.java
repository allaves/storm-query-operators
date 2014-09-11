package topology.trident;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.testing.FixedBatchSpout;
import utils.StreamPrinter;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/*
 * Trident example for non windowed join 
 */
public class TridentJoinExample {
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		
		// This spouts emits the same observations over and over again
		FixedBatchSpout observationSpout = new FixedBatchSpout(new Fields("obsId", "observedProperty", "value", "uom", "timestamp", "sensorId"), 5,
				new Values("obs1", "temperature", 35.0, "degrees Celsius", System.currentTimeMillis(), 1), 
				new Values("obs2", "temperature", 33.3, "degrees Celsius", System.currentTimeMillis(), 2), 
				new Values("obs3", "temperature", 34.1, "degrees Celsius", System.currentTimeMillis(), 3),
				new Values("obs4", "temperature", 35.2, "degrees Celsius", System.currentTimeMillis(), 4),
				new Values("obs5", "temperature", 31.4, "degrees Celsius", System.currentTimeMillis(), 5));
		observationSpout.setCycle(true);
		
		// This spouts emits the same sensor locations over and over again
		FixedBatchSpout sensorSpout = new FixedBatchSpout(new Fields("sensorId", "lat", "lon"), 5, 
				new Values(1, "40.4055381", "-3.8399521"), 
				new Values(2, "40.4055382", "-3.8399522"),
				new Values(3, "40.4055383", "-3.8399523"),
				new Values(4, "40.4055384", "-3.8399524"),
				new Values(5, "40.4055385", "-3.8399525"),
				new Values(6, "40.4055386", "-3.8399526"),
				new Values(7, "40.4055387", "-3.8399527"));
		sensorSpout.setCycle(true);
		
		// Topology, state, and streams definition
		TridentTopology tridentTopology = new TridentTopology();
		Stream observationStream = tridentTopology.newStream("observationStream", observationSpout);
		Stream sensorStream = tridentTopology.newStream("sensorStream", sensorSpout);
		tridentTopology.join(observationStream, new Fields("sensorId"), sensorStream, new Fields("sensorId"), 
				new Fields("sensorId", "obsId", "observedProperty", "value", "uom", "timestamp", "lat", "lon")).
				each(new Fields("sensorId", "obsId", "observedProperty", "value", "uom", "timestamp", "lat", "lon"), new StreamPrinter(),
						new Fields("sensorId2", "obsId2", "observedProperty2", "value2", "uom2", "timestamp2", "lat2", "lon2"));
		
		//TridentState windowedJoin = tridentTopology.join(observationStream, new Fields("sensorId"), sensorStream, new Fields("sensorId"), 
			//	new Fields("sensorId", "obsId", "observedProperty", "value", "uom", "timestamp", "lat", "lon")).
			//	persistentAggregate(MapState, new SimpleHashJoin(), new Fields("sensorId", "observedProperty", "value", "uom", "timestamp", "lat", "lon"));
		
		
		Config conf = new Config();
	    conf.setDebug(true);
		
		LocalCluster cluster = new LocalCluster();
		
	    cluster.submitTopology("tridentJoinExample", conf, tridentTopology.build());
	    
	    Utils.sleep(10000);
	    cluster.shutdown();
	}
}
	


