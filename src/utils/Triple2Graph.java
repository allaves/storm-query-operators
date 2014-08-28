package utils;

import java.util.Map;

import org.apache.jena.riot.RDFDataMgr;

import backtype.storm.tuple.Values;

import com.hp.hpl.jena.graph.Factory;
import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.impl.GraphBase;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.SimpleSelector;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.reasoner.TriplePattern;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

/*
 * Function that receives triples and emits graphs with name and timestamp
 */
public class Triple2Graph extends BaseFunction {
	
	private static final long serialVersionUID = -3772084644329582586L;
	
	private Graph graph;
	private SimpleSelector statementPattern;
	private String graphName;
	
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		super.prepare(conf, context);
		graph = Factory.createDefaultGraph();
		// TODO: Get the triple pattern from the configuration/context 
		statementPattern = new SimpleSelector(null, 
				ResourceFactory.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
				ResourceFactory.createResource("http://purl.oclc.org/NET/ssnx/ssn#FeatureOfInterest")); 
//		System.out.println("PRINTED PATTERN: " + statementPattern.toString());
//		Statement aux = ResourceFactory.createStatement(ResourceFactory.createResource("http://earthquakes.linkeddata.es/1185937"), 
//				ResourceFactory.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), 
//				ResourceFactory.createResource("http://purl.oclc.org/NET/ssnx/ssn#FeatureOfInterest"));
//		System.out.println("PRINTED TRIPLE: " + aux);
//		System.out.println("MATCHING?? " + statementPattern.test(aux));
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		Statement newStatement = ResourceFactory.createStatement(ResourceFactory.createResource(tuple.getString(0)), 
				ResourceFactory.createProperty(tuple.getString(1)),
				ResourceFactory.createResource(tuple.getString(2)));
		// The name of the graph is stored and added to the tuple at the emission
		// If the new triple matches the starting pattern and the graph is not empty, the graph is emitted.
		if (statementPattern.test(newStatement)) {
			if (!graph.isEmpty()) {
				// The values emitted correspond to the name of the graph (earthquake URI), the timestamp of creation, and the graph.
				collector.emit(new Values(graphName, System.currentTimeMillis(), graph));
				graph.clear();
			}
			graphName = tuple.getString(0);
		}
		graph.add(newStatement.asTriple());
	}

}
