package utils;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class GroupByTime extends BaseFunction {
	
	private static final long serialVersionUID = 6244951880347273178L;
	
	private long windowTimeInSeconds;
	
	public GroupByTime(long windowTimeInSeconds) {
		this.windowTimeInSeconds = windowTimeInSeconds;
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		long graphTimestamp = tuple.getLong(0);
		long timeGroup = graphTimestamp - (graphTimestamp % (this.windowTimeInSeconds * 1000));
		collector.emit(new Values(timeGroup, tuple.get(1)));
	}

}
