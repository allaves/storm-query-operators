package bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleMatch;
import com.hp.hpl.jena.rdf.model.SimpleSelector;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.syntax.ElementTriplesBlock;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class RDFSimpleJoinBolt extends BaseRichBolt {

	private static final long serialVersionUID = -6606787987861179871L;
	
	private OutputCollector collector;
	private TripleMatch attr;
	private SimpleSelector joinAttribute;
	private List<Graph> left;
	private List<Graph> right;

	
	public RDFSimpleJoinBolt(SimpleSelector joinAttribute, TripleMatch attr) {
		this.joinAttribute = joinAttribute;
		this.left = new ArrayList<Graph>();
		this.right = new ArrayList<Graph>();
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,	OutputCollector collector) {
		this.collector = collector;
		// Create a BGP from the query pattern
		// Having a triple <s, p, o> in the WHERE clause...
		Triple triple = Triple.create(Var.alloc("s"), Var.alloc("p"), Var.alloc("o"));
		BasicPattern pattern = new BasicPattern();
		pattern.add(triple);
		Op op = new OpBGP(pattern);
		//...
	}

	@Override
	public void execute(Tuple input) {
		if (left.isEmpty()) {
			left = (List<Graph>) input.getValue(0);
			collector.ack(input);
		}
		else {
			right = (List<Graph>) input.getValue(0);
			collector.ack(input);
			List<Graph> results = joinLeftRight();
			collector.emit(new Values(results));
		}
	}

	/*
	 * Nested loop join
	 */
	private List<Graph> joinLeftRight() {
		List<Graph> results = new ArrayList<Graph>();
		for (Graph l : left) {
			for (Graph r : right) {
				if (l.)
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//declarer.declare(new Fields());
	}

}
