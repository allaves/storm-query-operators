package topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.tuple.TridentTuple;

public class TridentJoinExample {
	
	public static void main(String[] args) {
		
		// This spouts emits the same observations over and over again
		FixedBatchSpout observationSpout = new FixedBatchSpout(new Fields("obsId", "observedProperty", "value", "uom", "timestamp", "sensorId"), 3,
				new Values(1, "temperature", 35.0, "degrees Celsius", System.currentTimeMillis(), "sensor1"), 
				new Values(2, "temperature", 33.3, "degrees Celsius", System.currentTimeMillis(), "sensor5"), 
				new Values(3, "temperature", 34.1, "degrees Celsius", System.currentTimeMillis(), "sensor2"),
				new Values(4, "temperature", 35.2, "degrees Celsius", System.currentTimeMillis(), "sensor1"),
				new Values(5, "temperature", 31.4, "degrees Celsius", System.currentTimeMillis(), "sensor2"));
		observationSpout.setCycle(true);
		
		// This spouts emits the same sensor locations over and over again
		FixedBatchSpout sensorSpout = new FixedBatchSpout(new Fields("sensorId", "lat", "lon"), 3, 
				new Values("sensor1", "40.4055381", "-3.8399521"), 
				new Values("sensor2", "40.4055382", "-3.8399522"),
				new Values("sensor3", "40.4055383", "-3.8399523"),
				new Values("sensor4", "40.4055384", "-3.8399524"),
				new Values("sensor5", "40.4055385", "-3.8399525"),
				new Values("sensor6", "40.4055386", "-3.8399526"),
				new Values("sensor7", "40.4055387", "-3.8399527"));
		sensorSpout.setCycle(true);
		
		// Topology and stream definition
		TridentTopology tridentTopology = new TridentTopology();
		Stream observationStream = tridentTopology.newStream("observationStream", observationSpout);
		Stream sensorStream = tridentTopology.newStream("sensorStream", observationSpout);
		tridentTopology.join(observationStream, new Fields("sensorId"), sensorStream, new Fields("sensorId"), 
				new Fields("obsId", "observedProperty", "value", "uom", "timestamp", "lat", "lon"));
		
		
		Config conf = new Config();
	    conf.setDebug(true);
		
		LocalCluster cluster = new LocalCluster();
		
		// Cannot see the joins
	    cluster.submitTopology("tridentJoinExample", conf, tridentTopology.build());
	    
	    Utils.sleep(5000);
	    cluster.shutdown();
	}
	
	
	// Filters observations above 35 ºC
	public class TemperatureFilter extends BaseFunction {
		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			if (tuple.getFloatByField("value") < 35.0) {
				collector.emit(tuple);
			}
		}
	}
}
	


