package utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import storm.trident.operation.BaseAggregator;
import storm.trident.operation.ReducerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.aggregate.Accumulator;
import com.hp.hpl.jena.sparql.expr.aggregate.Aggregator;
import com.hp.hpl.jena.sparql.expr.aggregate.AggregatorBase;
import com.hp.hpl.jena.sparql.graph.NodeTransform;

public class SlidingWindowAggregator implements ReducerAggregator<HashMap<String, Graph>> {

	private static final long serialVersionUID = 659372530551907859L;

	@Override
	public HashMap<String, Graph> init() {
		return new HashMap<String, Graph>();
	}

	@Override
	public HashMap<String, Graph> reduce(HashMap<String, Graph> map, TridentTuple tuple) {
		String id = tuple.get(0).toString();
		Graph g = (Graph) tuple.get(1);
		map.put(id, g);
		return map;
	}

	
	
	
}
