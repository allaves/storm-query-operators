package bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleMatch;
import com.hp.hpl.jena.rdf.model.SimpleSelector;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.syntax.ElementTriplesBlock;

import backtype.storm.Config;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.RotatingMap;

/*
 * Adapted from https://github.com/nathanmarz/storm-starter/blob/master/src/jvm/storm/starter/bolt/SingleJoinBolt.java
 * Source: storm-starter project
 */
public class RDFSimpleJoinBolt extends BaseRichBolt {

	private static final long serialVersionUID = -6606787987861179871L;
	
	private Fields idFields;
	private Fields outputFields;
	private int numSources;
	//private RotatingMap<List<Object>, Map<GlobalStreamId, Tuple>> pending;
	private RotatingMap<List<Object>, Map<GlobalStreamId, ArrayList<List<Object>>>> pending;
	private Map<String, GlobalStreamId> fieldLocations;
	
	private OutputCollector collector;
//	private TripleMatch attr;
//	//private SimpleSelector joinAttribute;
	private String joinAttribute;
//	private List<Graph> left;
//	private List<Graph> right;

	
	public RDFSimpleJoinBolt(String joinAttribute, TripleMatch attr, Fields outputFields) {
		this.outputFields = outputFields;
		this.joinAttribute = joinAttribute;
//		this.left = new ArrayList<Graph>();
//		this.right = new ArrayList<Graph>();
	}


	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.fieldLocations = new HashMap<String, GlobalStreamId>();
		this.collector = collector;
		int timeout = ((Number) conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS)).intValue();
		this.pending = new RotatingMap<List<Object>, Map<GlobalStreamId, ArrayList<List<Object>>>>(timeout);
		this.numSources = context.getThisSources().size();
		Set<String> idFields = null;
		for (GlobalStreamId source : context.getThisSources().keySet()) {
			Fields fields = context.getComponentOutputFields(source.get_componentId(), source.get_streamId());
			Set<String> setFields = new HashSet<String>(fields.toList());
			if (idFields == null) {
				idFields = setFields;
			}
			else {
				idFields.retainAll(setFields);
			}
			for (String outputfield : this.outputFields) {
				for (String sourcefield : fields) {
					if (outputfield.equals(sourcefield)) {
						this.fieldLocations.put(outputfield, source);
					}
				}
			}
		}
		this.idFields = new Fields(new ArrayList<String>(idFields));
		if (this.fieldLocations.size() != this.outputFields.size()) {
			throw new RuntimeException("Cannot find all outfields among sources");
		}
	}

	@Override
	public void execute(Tuple input) {
		ArrayList<Graph> graphList = (ArrayList<Graph>) input.getValue(0);
		ArrayList<List<Object>> tupleList = new ArrayList<List<Object>>();
		ArrayList<Object> tuple = null;
		List<Object> id = new ArrayList<Object>();
		Iterator<Triple> it = null;
		Triple triple = null;
		for (Graph g : graphList) {
			it = g.find(new Triple(null, null, null));
			tuple = new ArrayList<Object>();
			while(it.hasNext()) {
				triple = it.next();
				String s = triple.getSubject().toString();
				String p = triple.getPredicate().toString();
				String o = triple.getObject().toString();
				if (!id.isEmpty() && id.contains(p)) {
					id.add(triple.getPredicate());
				}
				tuple.add(s + " " + o);
			}
			tupleList.add(tuple);
		}
	    GlobalStreamId streamId = new GlobalStreamId(input.getSourceComponent(), input.getSourceStreamId());
	    if (!pending.containsKey(id)) {
	    	//pending.put(id, new HashMap<GlobalStreamId, Tuple>());
	    	pending.put(id, new HashMap<GlobalStreamId, ArrayList<List<Object>>>());
	    }
	    Map<GlobalStreamId, ArrayList<List<Object>>> parts = pending.get(id);
	    if (parts.containsKey(streamId))
	    	throw new RuntimeException("Received same side of single join twice");
	    //parts.put(streamId, tuple);
	    parts.put(streamId, tupleList);
	    if (parts.size() == numSources) {
	    	pending.remove(id);
	    	ArrayList<Object> joinResult = new ArrayList<Object>();
	    	for (String outField : outputFields) {
	    		GlobalStreamId loc = fieldLocations.get(outField);
	    		//joinResult.add(parts.get(loc).getValueByField(outField));
	    		joinResult.add(parts.get(loc).get(id.indexOf(outField)));
	    	}
	    	collector.emit(joinResult);
	    	collector.ack(input);
    	}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    declarer.declare(outputFields);
	}
	
	
	

}
