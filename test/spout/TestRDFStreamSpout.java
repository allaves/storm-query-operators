package spout;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.jena.atlas.lib.Sink;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.lang.PipedRDFIterator;
import org.apache.jena.riot.lang.PipedRDFStream;
import org.apache.jena.riot.lang.PipedTriplesStream;
import org.apache.jena.riot.out.SinkTripleOutput;
import org.apache.jena.riot.system.SyntaxLabels;

import com.hp.hpl.jena.graph.Triple;

public class TestRDFStreamSpout {

	public static void main(String[] args) {
		final String fileName = "data/Earthquakes-Spain-2013.ttl";		

		Sink<Triple> output = new SinkTripleOutput(System.out, null, SyntaxLabels.createNodeToLabel());
		PipedRDFIterator<Triple> iterator = new PipedRDFIterator<Triple>();
        final PipedRDFStream<Triple> inputStream = new PipedTriplesStream(iterator);

		//RDFDataMgr.parse(inputStream, fileName);
		
		// PipedRDFStream and PipedRDFIterator need to be on different threads
        ExecutorService executor = Executors.newSingleThreadExecutor();

        // Create a runnable for our parser thread
        Runnable parser = new Runnable() {

            @Override
            public void run() {
                // Call the parsing process.
                RDFDataMgr.parse(inputStream, fileName);
            }
        };

        // Start the parser on another thread
        executor.submit(parser);
		
//		
//		// Iterate over the data
		while (iterator.hasNext()) {
			Triple next = iterator.next();
			System.out.println("STRING: "+ next.toString());
			//output.send(next);
		}
		
		
	}

}
