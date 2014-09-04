package bolt;

import java.util.Date;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;

import com.hp.hpl.jena.graph.Graph;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;


public class GraphPrinterBolt extends BaseBasicBolt {

	
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
	    System.out.println("PRINTER BOLT (" + new Date() + "): " + tuple);
	    RDFDataMgr.write(System.out, (Graph) tuple.getValue(2), Lang.N3);
	    collector.emit(tuple.getValues());
	}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("name", "timestamp", "graph"));
    }

}
