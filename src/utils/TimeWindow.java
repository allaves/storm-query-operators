package utils;

import backtype.storm.Constants;
import backtype.storm.tuple.Tuple;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class TimeWindow extends BaseFunction {

	private static final long serialVersionUID = -2461794117408969615L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		if (isTickTuple(tuple)) {
			finishBatch();
		}
		else {
			tuples.add(tuple);
			collector.ack(tuple);
		}
	}

	/*
	 * Returns true if the argument is a tick tuple and false otherwise
	 */
	private boolean isTickTuple(TridentTuple tuple) {
		return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) &&
				tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
	}

	/*
	 * Emit the list of tuples when the window time expires
	 */
	public void finishBatch() {
		collector.emit(tuples);
	}

}
