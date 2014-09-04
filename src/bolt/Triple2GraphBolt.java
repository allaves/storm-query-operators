package bolt;

import java.util.Map;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;

import com.hp.hpl.jena.graph.Factory;
import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.SimpleSelector;
import com.hp.hpl.jena.rdf.model.Statement;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class Triple2GraphBolt extends BaseRichBolt {

	private static final long serialVersionUID = -7697879069045076878L;
	
	private OutputCollector collector;
	private Graph graph;
	private SimpleSelector statementPattern;
	private String graphName;
	
	@Override
	public void prepare(Map conf, TopologyContext context,	OutputCollector collector) {
		this.collector = collector;
		graph = Factory.createDefaultGraph();
		// TODO: Get the triple pattern from the configuration/context 
		statementPattern = new SimpleSelector(null, 
				ResourceFactory.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
				ResourceFactory.createResource("http://purl.oclc.org/NET/ssnx/ssn#FeatureOfInterest")); 
	}

	@Override
	public void execute(Tuple tuple) {
		Statement newStatement = ResourceFactory.createStatement(ResourceFactory.createResource(tuple.getString(0)), 
				ResourceFactory.createProperty(tuple.getString(1)),
				ResourceFactory.createResource(tuple.getString(2)));
		// The name of the graph is stored and added to the tuple at the emission
		// If the new triple matches the starting pattern and the graph is not empty, the graph is emitted.
		if (statementPattern.test(newStatement)) {
			if (!graph.isEmpty()) {
				// The values emitted correspond to the name of the graph (earthquake URI), the timestamp of creation, and the graph.
				System.out.println("GRAPH BEFORE BEING EMITTED:");
				RDFDataMgr.write(System.out, graph, Lang.N3);
				collector.emit(new Values(graphName, System.currentTimeMillis(), graph));
				graph.clear();
			}
			graphName = tuple.getString(0);
		}
		graph.add(newStatement.asTriple());
		collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("name", "timestamp", "graph"));
	}

}
