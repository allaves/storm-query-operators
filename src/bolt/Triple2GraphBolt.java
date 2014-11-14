package bolt;

import java.util.HashMap;
import java.util.Map;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;

import com.hp.hpl.jena.graph.Factory;
import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.SimpleSelector;
import com.hp.hpl.jena.rdf.model.Statement;

import backtype.storm.Config;
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
	private String startingPatternId;
	private SimpleSelector startingPattern;
	private String graphName;
	
	public Triple2GraphBolt(String startingPatternKey) {
		this.startingPatternId = startingPatternKey;
	}
	
	@Override
	public void prepare(Map conf, TopologyContext context,	OutputCollector collector) {
		this.collector = collector;
		graph = Factory.createDefaultGraph();
		startingPatternId = (String) conf.get("STARTING_PATTERN_ID");
		Resource subject = null;
		if ((conf.get(startingPatternId + "_SUBJECT")) != null) {
			subject = ResourceFactory.createResource((String) conf.get(startingPatternId + "_SUBJECT"));
		}
		Property predicate = null;
		if ((conf.get(startingPatternId + "_PREDICATE")) != null) {
			predicate = ResourceFactory.createProperty((String) conf.get(startingPatternId + "_PREDICATE"));
		}
		Resource object = null;
		if ((conf.get(startingPatternId + "_OBJECT")) != null) {
			object = ResourceFactory.createProperty((String) conf.get(startingPatternId + "_OBJECT"));
		}
		startingPattern = new SimpleSelector(subject, predicate, object);
	}

	@Override
	public void execute(Tuple tuple) {
		Statement newStatement = ResourceFactory.createStatement(ResourceFactory.createResource(tuple.getString(0)), 
				ResourceFactory.createProperty(tuple.getString(1)),
				ResourceFactory.createResource(tuple.getString(2)));
		// The name of the graph is stored and added to the tuple at the emission
		// If the new triple matches the starting pattern and the graph is not empty, the graph is emitted.
		if (startingPattern.test(newStatement)) {
			if (!graph.isEmpty()) {
				// The values emitted correspond to the name of the graph (earthquake URI), the timestamp of creation, and the graph.
//				RDFDataMgr.write(System.out, graph, Lang.N3);
				collector.emit(new Values(graphName, System.currentTimeMillis(), graph));
				System.out.println("EMITTED GRAPH: " + graphName);
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
	
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
	    Map<String, Object> conf = new HashMap<String, Object>();
	    return conf;
	}

}
