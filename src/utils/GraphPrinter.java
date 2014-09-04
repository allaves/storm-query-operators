package utils;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;

import com.hp.hpl.jena.graph.Graph;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class GraphPrinter extends BaseFunction {

	private static final long serialVersionUID = -4263469506208685214L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		RDFDataMgr.write(System.out, (Graph) tuple.get(0), Lang.N3);
		collector.emit(tuple);
	}

}
