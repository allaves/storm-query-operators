package topology.storm;

import spout.RDFStreamSpout;
import storm.starter.bolt.IntermediateRankingsBolt;
import storm.starter.bolt.PrinterBolt;
import storm.starter.bolt.RollingCountBolt;
import storm.starter.bolt.TotalRankingsBolt;
import storm.starter.util.StormRunner;
import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolt.AckerPrinterBolt;
import bolt.GraphPrinterBolt;
import bolt.RollingWindowBolt;
import bolt.Triple2GraphBolt;

/*
 * This topology counts how many different graphs appear in 15-seconds windows. Results are emitted every 3 seconds. 
 */
public class GraphCounterTopology {
	
	private static final int DEFAULT_RUNTIME_IN_SECONDS = 60;
	private static final int TOP_N = 5;
	
	private TopologyBuilder builder;
	private String topologyName;
	private Config topologyConfig;
	private int runtimeInSeconds;
	
	public GraphCounterTopology() {
		builder = new TopologyBuilder();
		topologyName = "graphCounterTopology";
		topologyConfig = createTopologyConfiguration();
		runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;
		
		wireTopology();
	}

	private void wireTopology() {
		
		String fileName = "data/Earthquakes-Spain-2013.ttl";
		String spoutId = "rdfStreamSpout";
		String triple2graph = "triple2graph";
		String graphCounter = "graphCounter";
		String finalCounter = "finalCounter";
		
		builder.setSpout(spoutId, new RDFStreamSpout(fileName));
		builder.setBolt(triple2graph, new Triple2GraphBolt()).shuffleGrouping(spoutId);
		builder.setBolt(graphCounter, new RollingCountBolt(15, 3)).fieldsGrouping(triple2graph, new Fields("name"));
		builder.setBolt(finalCounter, new AckerPrinterBolt()).globalGrouping(graphCounter);
	}

	private Config createTopologyConfiguration() {
		Config conf = new Config();
		//conf.setDebug(true);
		return conf;
	}
	
	public void run() throws InterruptedException {
		StormRunner.runTopologyLocally(builder.createTopology(), topologyName, topologyConfig, runtimeInSeconds);
	}
	
	public static void main(String[] args) throws Exception {
		new GraphCounterTopology().run();
	}

}
