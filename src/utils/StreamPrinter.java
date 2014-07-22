package utils;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class StreamPrinter extends BaseFunction {

	private static final long serialVersionUID = -4263469506208685214L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		System.out.println("Joined tuple: " + tuple.toString());
		collector.emit(tuple);
	}

}
