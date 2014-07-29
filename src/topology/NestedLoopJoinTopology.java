package topology;

import spout.RDFStreamSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolt.NestedLoopIteratorBolt;
import bolt.TimeWindowBolt;

public class NestedLoopJoinTopology {
	
	private static int firstTimeWindow = 10;
	private static int secondTimeWindow = 30;
	
	public static void main(String[] args) {
		//FeederSpout testSpout
		
		TopologyBuilder builder = new TopologyBuilder();
		//builder.setSpout("testSpout1", new RDFStreamSpout());
		//builder.setBolt("timeWindow1", new TimeWindowBolt()).shuffleGrouping("testSpout1");
		//builder.setBolt("timeWindow2", new TimeWindowBolt()).shuffleGrouping("testSpout2");
		builder.setBolt("nestedLoopIterator", new NestedLoopIteratorBolt()).shuffleGrouping("timeWindow1", "timeWindow2");
		
		//builder.setBolt("displayObservations", new DisplayHSLVehiclesBolt(), 8).fieldsGrouping("separateFields", new Fields("vehicleId"));
		
		Config conf = new Config();
		conf.setDebug(true);
		conf.setNumWorkers(2);
		conf.put("FIRST_TIME_WINDOW", firstTimeWindow);
		conf.put("SECOND_TIME_WINDOW", secondTimeWindow);
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("hslVehicleTracking", conf, builder.createTopology());
	}

}
