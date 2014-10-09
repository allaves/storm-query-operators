package bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.jena.riot.RDFDataMgr;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpAsQuery;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.engine.main.OpExecutor;
import com.hp.hpl.jena.sparql.util.FmtUtils;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class OpBGPBolt extends BaseRichBolt {
	
	private Fields outputFields;
	private List<Triple> triplesPattern;
	private OutputCollector collector;
	private Op opBGP;
	
	//public OpBGPBolt(Fields outputFields, List<Triple> triplesPattern) {
	public OpBGPBolt(Fields outputFields, List<String> stringPattern) {
		this.outputFields = outputFields;
		this.triplesPattern = new ArrayList<Triple>();
		for (String str : stringPattern) {
			String[] triple = str.split(" ");
			triplesPattern.add(Triple.create(Var.alloc(triple[0]), ResourceFactory.createProperty(triple[1]).asNode(), Var.alloc(triple[2])));
		}
		BasicPattern pattern = BasicPattern.wrap(triplesPattern);
		this.opBGP = new OpBGP(pattern);
//		Query q = OpAsQuery.asQuery(op);
//		q.setQuerySelectType();
//		OpExecutor executor = new OpExecutor(new ExecutionContext());
		
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,	OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	/*
	 * (non-Javadoc)
	 * @see backtype.storm.task.IBolt#execute(backtype.storm.tuple.Tuple)
	 * We assume that each tuple here is a set of graphs, result of the windowing bolt.
	 */
	public void execute(Tuple input) {
		List<Graph> graphList = (List<Graph>) input.getValue(0);
		QueryIterator queryIter = null;
		for (Graph g : graphList) {
			queryIter = Algebra.exec(this.opBGP, g); 
			while (queryIter.hasNext()) {
				Binding binding = queryIter.nextBinding();
				Var var = Var.alloc("obs");
				Node node = binding.get(var);
				System.out.println(var + " = " + FmtUtils.stringForNode(node));
				collector.emit(new Values(node));
			}
		}
		collector.ack(input);
	}
			

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(this.outputFields);
	}

}
