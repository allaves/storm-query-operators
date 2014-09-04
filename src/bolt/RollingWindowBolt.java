package bolt;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;
import storm.starter.tools.NthLastModifiedTimeTracker;
import storm.starter.tools.SlidingWindowCounter;
import storm.starter.util.TupleHelpers;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Based on https://github.com/nathanmarz/storm-starter/blob/master/src/jvm/storm/starter/bolt/RollingCountBolt.java
 */
public class RollingWindowBolt extends BaseRichBolt {

  private static final long serialVersionUID = 1761364280293026138L;
  
  private static final Logger LOG = Logger.getLogger(RollingWindowBolt.class);
  private static final int NUM_WINDOW_CHUNKS = 5;
  private static final int DEFAULT_SLIDING_WINDOW_IN_SECONDS = NUM_WINDOW_CHUNKS * 60;
  private static final int DEFAULT_EMIT_FREQUENCY_IN_SECONDS = DEFAULT_SLIDING_WINDOW_IN_SECONDS / NUM_WINDOW_CHUNKS;
  private static final String WINDOW_LENGTH_WARNING_TEMPLATE =
      "Actual window length is %d seconds when it should be %d seconds"
          + " (you can safely ignore this warning during the startup phase)";

  private final SlidingWindower<Object> counter;
  private final int windowLengthInSeconds;
  private final int emitFrequencyInSeconds;
  private OutputCollector collector;
  private NthLastModifiedTimeTracker lastModifiedTracker;

  public RollingWindowBolt() {
    this(DEFAULT_SLIDING_WINDOW_IN_SECONDS, DEFAULT_EMIT_FREQUENCY_IN_SECONDS);
  }

  public RollingWindowBolt(int windowLengthInSeconds, int emitFrequencyInSeconds) {
    this.windowLengthInSeconds = windowLengthInSeconds;
    this.emitFrequencyInSeconds = emitFrequencyInSeconds;
    counter = new SlidingWindowCounter<Object>(deriveNumWindowChunksFrom(this.windowLengthInSeconds,
        this.emitFrequencyInSeconds));
  }

  private int deriveNumWindowChunksFrom(int windowLengthInSeconds, int windowUpdateFrequencyInSeconds) {
    return windowLengthInSeconds / windowUpdateFrequencyInSeconds;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
    lastModifiedTracker = new NthLastModifiedTimeTracker(deriveNumWindowChunksFrom(this.windowLengthInSeconds,
        this.emitFrequencyInSeconds));
  }

  @Override
  public void execute(Tuple tuple) {
    if (TupleHelpers.isTickTuple(tuple)) {
      LOG.debug("Received tick tuple, triggering emit of current window counts");
      emitCurrentWindowCounts();
    }
    else {
      countObjAndAck(tuple);
    }
  }

  private void emitCurrentWindowCounts() {
    Map<Object, Long> counts = counter.getCountsThenAdvanceWindow();
    int actualWindowLengthInSeconds = lastModifiedTracker.secondsSinceOldestModification();
    lastModifiedTracker.markAsModified();
    if (actualWindowLengthInSeconds != windowLengthInSeconds) {
      LOG.warn(String.format(WINDOW_LENGTH_WARNING_TEMPLATE, actualWindowLengthInSeconds, windowLengthInSeconds));
    }
    emit(counts, actualWindowLengthInSeconds);
  }

  private void emit(Map<Object, Long> counts, int actualWindowLengthInSeconds) {
    for (Entry<Object, Long> entry : counts.entrySet()) {
      Object obj = entry.getKey();
      Long count = entry.getValue();
      collector.emit(new Values(obj, count, actualWindowLengthInSeconds));
    }
  }

  private void countObjAndAck(Tuple tuple) {
    Object obj = tuple.getValue(0);
    counter.incrementCount(obj);
    collector.ack(tuple);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("obj", "count", "actualWindowLengthInSeconds"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    Map<String, Object> conf = new HashMap<String, Object>();
    conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
    return conf;
  }
}
