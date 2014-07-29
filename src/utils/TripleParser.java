package utils;

import org.apache.jena.riot.RDFDataMgr;

import backtype.storm.tuple.Values;

import com.hp.hpl.jena.graph.Triple;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;


public class TripleParser extends BaseFunction {

	private static final long serialVersionUID = 8216337330323251219L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		Triple triple = (Triple) tuple.get(0);
		
		collector.emit(new Values());
	}

}
