package topology.storm;

import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.SimpleSelector;

import spout.RDFStreamSpout;
import storm.starter.bolt.IntermediateRankingsBolt;
import storm.starter.bolt.PrinterBolt;
import storm.starter.bolt.RollingCountBolt;
import storm.starter.bolt.TotalRankingsBolt;
import storm.starter.util.StormRunner;
import storm.trident.testing.FixedBatchSpout;
import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import bolt.AckerPrinterBolt;
import bolt.GraphPrinterBolt;
import bolt.RollingWindowBolt;
import bolt.Triple2GraphBolt;

/*
 * This topology performs a join over 15-seconds windows. Results are emitted every 3 seconds. 
 */
public class RDFSimpleJoinStormExample {
	
	private static final int DEFAULT_RUNTIME_IN_SECONDS = 60;
	private static final int TOP_N = 5;
	
	private final String STARTING_PATTERN_ID = "STARTING_PATTERN";
	private final String STARTING_PATTERN_SUBJECT = null;
	private final String STARTING_PATTERN_PREDICATE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
	private final String STARTING_PATTERN_OBJECT = "http://purl.oclc.org/NET/ssnx/ssn#FeatureOfInterest";
	
	private TopologyBuilder builder;
	private String topologyName;
	private Config topologyConfig;
	private int runtimeInSeconds;
	
	public RDFSimpleJoinStormExample() {
		builder = new TopologyBuilder();
		topologyName = "graphCounterTopology";
		topologyConfig = createTopologyConfiguration();
		runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;
		
		wireTopology();
	}

	private void wireTopology() {
		
		String fileName = "data/Earthquakes-Spain-2013.ttl";
		//String rdfSpout = "rdfStreamSpout";
		//String fixedBatchSpout = "fixedBatchSpout";
		//String triple2graph = "triple2graph";
		String graphCounter = "graphCounter";
		String finalCounter = "finalCounter";
		
		builder.setSpout("rdfSpout1", new RDFStreamSpout(fileName));
		builder.setSpout("rdfSpout2", new RDFStreamSpout(fileName));
		builder.setBolt("triple2graph1", new Triple2GraphBolt(STARTING_PATTERN_ID)).shuffleGrouping("rdfSpout1");
		builder.setBolt("triple2graph2", new Triple2GraphBolt(STARTING_PATTERN_ID)).shuffleGrouping("rdfSpout2");
		builder.setBolt("graphCounter1", new RollingCountBolt(15, 3)).fieldsGrouping("triple2graph1", new Fields("name"));
		builder.setBolt("graphCounter2", new RollingCountBolt(15, 3)).fieldsGrouping("triple2graph2", new Fields("name"));
		builder.setBolt(finalCounter, new AckerPrinterBolt()).globalGrouping("graphCounter1").globalGrouping("graphCounter2");
	}

	private Config createTopologyConfiguration() {
		Config conf = new Config();
		//conf.setDebug(true);
		conf.put("STARTING_PATTERN_ID", this.STARTING_PATTERN_ID);
		conf.put("STARTING_PATTERN_SUBJECT", this.STARTING_PATTERN_SUBJECT);
		conf.put("STARTING_PATTERN_PREDICATE", this.STARTING_PATTERN_PREDICATE);
		conf.put("STARTING_PATTERN_OBJECT", this.STARTING_PATTERN_OBJECT);
		return conf;
	}
	
	public void run() throws InterruptedException {
		StormRunner.runTopologyLocally(builder.createTopology(), topologyName, topologyConfig, runtimeInSeconds);
	}
	
	public static void main(String[] args) throws Exception {
		new RDFSimpleJoinStormExample().run();
	}

}
