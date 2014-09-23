package topology.storm;

import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.SimpleSelector;

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
	
	private static final String STARTING_PATTERN_ID = "STARTING_PATTERN";
	private static final String STARTING_PATTERN_SUBJECT = null;
	private static final String STARTING_PATTERN_PREDICATE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
	private static final String STARTING_PATTERN_OBJECT = "http://purl.oclc.org/NET/ssnx/ssn#FeatureOfInterest";
	
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
		
		// TODO: Get the triple pattern from the configuration/context 
		SimpleSelector startingPattern = new SimpleSelector(null, 
				ResourceFactory.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
				ResourceFactory.createResource("http://purl.oclc.org/NET/ssnx/ssn#FeatureOfInterest"));
		
		builder.setSpout(spoutId, new RDFStreamSpout(fileName));
		builder.setBolt(triple2graph, new Triple2GraphBolt(STARTING_PATTERN_ID)).shuffleGrouping(spoutId);
		builder.setBolt(graphCounter, new RollingCountBolt(15, 3)).fieldsGrouping(triple2graph, new Fields("name"));
		builder.setBolt(finalCounter, new AckerPrinterBolt()).globalGrouping(graphCounter);
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
		new GraphCounterTopology().run();
	}

}
