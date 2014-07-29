package spout;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.lang.PipedRDFIterator;
import org.apache.jena.riot.lang.PipedRDFStream;
import org.apache.jena.riot.lang.PipedTriplesStream;

import com.hp.hpl.jena.graph.Triple;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/*
 * Spout for RDF streams
 * Based on https://svn.apache.org/repos/asf/jena/trunk/jena-arq/src-examples/arq/examples/riot/ExRIOT_6.java
 */
public class RDFStreamSpout extends BaseRichSpout {

	private static final long serialVersionUID = 8966458505147153573L;
	
	private String filePath;
	private int timeWindowInSeconds;
	private SpoutOutputCollector collector;
	private PipedRDFIterator<Triple> iterator;
	
	public RDFStreamSpout(String filePath, int timeWindowInSeconds) {
		this.filePath = filePath;
		this.timeWindowInSeconds = timeWindowInSeconds;
	}

	@Override
	public void open(Map conf, TopologyContext context,	SpoutOutputCollector collector) {
		this.collector = collector;
		
		// Create an RDF stream and an iterator
		iterator = new PipedRDFIterator<Triple>();
		final PipedRDFStream<Triple> inputStream = new PipedTriplesStream(iterator);
		
		// PipedRDFStream and PipedRDFIterator need to be in different threads to avoid deadlocks
		ExecutorService executor = Executors.newSingleThreadExecutor();
		
		// Runnable for the parser thread
		Runnable parser = new Runnable() {

			@Override
			public void run() {
				// Parsing process
				RDFDataMgr.parse(inputStream, filePath);
			}
		};
		
		// Start the parser on a different thread
		executor.submit(parser);
	}

	@Override
	public void nextTuple() {
		// Time between triple emission
		Utils.sleep(1000);
		
		// Iterate over the data
		while (iterator.hasNext()) {
			Triple next = iterator.next();
			collector.emit(new Values(next));
		}
	}

	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("triples"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, timeWindowInSeconds);
		return conf;
	}

}
