package spout;

import java.io.BufferedReader;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.jena.atlas.lib.Sink;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.lang.PipedRDFIterator;
import org.apache.jena.riot.lang.PipedRDFStream;
import org.apache.jena.riot.lang.PipedTriplesStream;
import org.apache.jena.riot.out.NodeToLabel;
import org.apache.jena.riot.out.SinkTripleOutput;
import org.apache.jena.riot.stream.StreamManager;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.SyntaxLabels;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

/*
 * Spout for RDF streams
 * Input source: file
 * Based on https://svn.apache.org/repos/asf/jena/trunk/jena-arq/src-examples/arq/examples/riot/ExRIOT_6.java
 */
public class RDFStreamSpout extends BaseRichSpout {

	private static final long serialVersionUID = 8966458505147153573L;
	
	private String filePath;
	private SpoutOutputCollector collector;
	private PipedRDFIterator<Triple> iterator; 
	
	public RDFStreamSpout(String filePath) {
		this.filePath = filePath;
	}

	@Override
	public void open(Map conf, TopologyContext context,	SpoutOutputCollector collector) {
		this.collector = collector;
		
		//Sink<Triple> output = new SinkTripleOutput(System.out, null, SyntaxLabels.createNodeToLabel());
		iterator = new PipedRDFIterator<Triple>();
        final PipedRDFStream<Triple> inputStream = new PipedTriplesStream(iterator);

		// PipedRDFStream and PipedRDFIterator need to be on different threads
        ExecutorService executor = Executors.newSingleThreadExecutor();

        // Create a runnable for our parser thread
        Runnable parser = new Runnable() {

            @Override
            public void run() {
                // Call the parsing process.
                RDFDataMgr.parse(inputStream, filePath, Lang.TURTLE);
            }
        };

        // Start the parser on another thread
        executor.submit(parser);
	}

	@Override
	public void nextTuple() {
		// Time between triple emission
		Utils.sleep(100);
		if (iterator.hasNext()) {
			Triple next = iterator.next();
			// Cannot remove the '@' from the predicate
			collector.emit(new Values(next.getSubject().toString(), next.getPredicate().toString(), next.getObject().toString()));
		}
		
		
	}

	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("s", "p", "o"));
	}

//	@Override
//	public Map<String, Object> getComponentConfiguration() {
//		Config conf = new Config();
//		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, timeWindowInSeconds);
//		return conf;
//	}

}
