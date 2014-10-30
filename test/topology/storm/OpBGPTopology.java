package topology.storm;

import java.util.ArrayList;
import java.util.List;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.sparql.core.Var;

import spout.RDFStreamSpout;
import storm.starter.bolt.RollingCountBolt;
import storm.starter.util.StormRunner;
import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolt.AckerPrinterBolt;
import bolt.OpBGPBolt;
import bolt.RollingWindowBolt;
import bolt.Triple2GraphBolt;

public class OpBGPTopology {
	
	private final String STARTING_PATTERN_ID = "STARTING_PATTERN";
	private final String STARTING_PATTERN_SUBJECT = null;
	private final String STARTING_PATTERN_PREDICATE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
	private final String STARTING_PATTERN_OBJECT = "http://purl.oclc.org/NET/ssnx/ssn#FeatureOfInterest";
	
	private TopologyBuilder builder;
	private String topologyName;
	private Config topologyConfig;
	private int runtimeInSeconds;
	//private List<Triple> triplesPattern;
	private ArrayList<String> triplesPattern;
	//private String triplesPattern;
	
	public OpBGPTopology() {
		builder = new TopologyBuilder();
		topologyName = "graphCounterTopology";
		topologyConfig = createTopologyConfiguration();
		runtimeInSeconds = 15;
		//triplesPattern = new ArrayList<Triple>();
		triplesPattern = new ArrayList<String>();
		// Testing
		triplesPattern.add("?obs http://www.w3.org/1999/02/22-rdf-syntax-ns#type http://purl.oclc.org/NET/ssnx/ssn#Observation");
		triplesPattern.add("?obs http://purl.oclc.org/NET/ssnx/ssn#observationResult ?sensorOutput");
		
//				triplesList.add(Triple.create(Var.alloc("obs"), ResourceFactory.createProperty("ssn:observationResult").asNode(), Var.alloc("sensorOutput")));
//				triplesList.add(Triple.create(Var.alloc("obs"), ResourceFactory.createProperty("ssn:observationSamplingTime").asNode(), Var.alloc("timestamp")));
//				triplesList.add(Triple.create(Var.alloc("obs"), ResourceFactory.createProperty("ssn:observedBy").asNode(), Var.alloc("sensor")));
//				triplesList.add(Triple.create(Var.alloc("sensorOutput"), ResourceFactory.createProperty("ssn:hasValue").asNode(), Var.alloc("value")));
//				triplesList.add(Triple.create(Var.alloc("obsValue"), ResourceFactory.createProperty("dul:hasDataValue").asNode(), Var.alloc("sensor")));
//				triplesList.add(Triple.create(Var.alloc("timestamp"), ResourceFactory.createProperty("time:inXSDDateTime").asNode(), Var.alloc("time")));
//				triplesList.add(Triple.create(Var.alloc("sensor"), ResourceFactory.createProperty("geo:hasGeometry").asNode(), Var.alloc("geometry")));
//				triplesList.add(Triple.create(Var.alloc("geometry"), ResourceFactory.createProperty("geo:asWKT").asNode(), Var.alloc("location")));
					
		wireTopology();
	}

	private void wireTopology() {
		String fileName = "data/Earthquakes-Spain-2013.ttl";
		//String rdfSpout = "rdfStreamSpout";
		//String fixedBatchSpout = "fixedBatchSpout";
		//String triple2graph = "triple2graph";
		
		builder.setSpout("rdfSpout1", new RDFStreamSpout(fileName));
		//builder.setSpout("rdfSpout2", new RDFStreamSpout(fileName));
		builder.setBolt("triple2graph1", new Triple2GraphBolt(STARTING_PATTERN_ID)).shuffleGrouping("rdfSpout1");
		//builder.setBolt("triple2graph2", new Triple2GraphBolt(STARTING_PATTERN_ID)).shuffleGrouping("rdfSpout2");
		builder.setBolt("graphCounter1", new RollingWindowBolt<Graph>(15, 3)).fieldsGrouping("triple2graph1", new Fields("name"));
		//builder.setBolt("graphCounter2", new RollingCountBolt(15, 3)).fieldsGrouping("triple2graph2", new Fields("name"));
		//builder.setBolt("bgpBolt", new OpBGPBolt("obs", triplesPattern)).shuffleGrouping("graphCounter1");
		builder.setBolt("bgpBolt", new OpBGPBolt(new Fields("obs"), triplesPattern), 1).globalGrouping("graphCounter1");
		//builder.setBolt("acker", new AckerPrinterBolt()).globalGrouping("graphCounter1").globalGrouping("graphCounter2");
		builder.setBolt("acker", new AckerPrinterBolt()).globalGrouping("bgpBolt");
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
		new OpBGPTopology().run();
	}

}
