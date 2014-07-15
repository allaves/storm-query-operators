package topology;

import java.sql.Date;
import java.text.DateFormat;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.testing.FeederSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import bolt.ExtendedSingleJoinBolt;

/*
 * Source: storm-starter project
 * Adapted from SingleJoinExample
 * https://github.com/nathanmarz/storm-starter/blob/master/src/jvm/storm/starter/SingleJoinExample.java
 */
public class ExtendedSingleJoinExample {
  public static void main(String[] args) {
	// Define the spouts
    FeederSpout observationSpout = new FeederSpout(new Fields("obsId", "observedProperty", "value", "uom", "timestamp", "sensorId"));
    FeederSpout sensorSpout = new FeederSpout(new Fields("sensorId", "lat", "lon"));

    // Define the topology
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("observation", observationSpout);
    builder.setSpout("sensor", sensorSpout);
    builder.setBolt("join", new ExtendedSingleJoinBolt(new Fields("observedProperty", "value", "uom", "timestamp", "lat", "lon")))
    	.fieldsGrouping("sensor", new Fields("sensorId")).fieldsGrouping("observation", new Fields("sensorId"));
    
    Config conf = new Config();
    conf.setDebug(true);

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("extended-join-example", conf, builder.createTopology());

    // Simulate data - Feed the spouts
    for (int i = 9; i >= 0; i--) {
    	sensorSpout.feed(new Values(i, "40.4055385", "-3.8399527"));
    }
    
    for (int i = 0; i < 50; i++) {
      observationSpout.feed(new Values(i, "temperature", Math.random()%10, "degrees Celsius", new Date(System.currentTimeMillis()), (i+1)/10));
    }

    Utils.sleep(2000);
    cluster.shutdown();
  }
}
