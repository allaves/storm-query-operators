package join;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.OutputFieldsGetter;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.tuple.Values;
import junit.framework.TestCase;

public class TestNestedLoopJoinOperator extends TestCase {
	
	private LinkedList<Tuple> s, r;

	public TestNestedLoopJoinOperator(String name) {
		super(name);
		// Create lists (tables)
		s = new LinkedList<Tuple>();
		r = new LinkedList<Tuple>();
		
		// Storm configuration
		//OutputFieldsDeclarer declarer = new OutputFieldsGetter();
		//declarer.declare(new Fields("id", "value", "uom"));
		
		// Populate listS
		List<Object> listS = new ArrayList<Object>();
		// 1st row
		listS.add("obs1");
		listS.add(34);
		listS.add("ºC");
		s.add(new TupleImpl(null, listS, 0, "test"));
		listS.clear();
		// 2nd row
		listS.add("obs2");
		listS.add(32);
		listS.add("ºC");
		s.add(new TupleImpl(null, listS, 0, "test"));
		listS.clear();
		// 3rd row
		listS.add("obs3");
		listS.add(35);
		listS.add("ºC");
		s.add(new TupleImpl(null, listS, 0, "test"));
		listS.clear();
		
		// Populate listR
		List<Object> listR = new ArrayList<Object>();
		// 1st row
		listR.add("obs1");
		listR.add("temperature");
		listR.add("celsius");
		listR.add("sensor1");
		r.add(new TupleImpl(null, listR, 0, "test"));
		listR.clear();
		// 2nd row
		listR.add("obs3");
		listR.add("temperature");
		listR.add("Kelvin");
		listR.add("sensor2");
		r.add(new TupleImpl(null, listR, 0, "test"));
		listR.clear();
		// 3rd row
		listR.add("obs4");
		listR.add("temperature");
		listR.add("Fahrenheit");
		listR.add("sensor3");
		r.add(new TupleImpl(null, listR, 0, "test"));
		listR.clear();
		// 4th row
		listR.add("obs5");
		listR.add("moisture");
		listR.add("%");
		listR.add("sensor5");
		r.add(new TupleImpl(null, listR, 0, "test"));
		
		//Tuple tuple = new TupleImpl(null, listR, 0, "test");
	
		
		// Show tables
		showTable(s);
		showTable(r);
		
		// Execute Join
		testExecute(r, s);
		
		// Show results
	}
	
	private void showTable(LinkedList<Tuple> table) {
		System.out.println(table);
	}

	public void testExecute(LinkedList<Tuple> s, LinkedList<Tuple> r) {
		//NestedLoopJoinOperator.execute(s, r, "id", conditionS, conditionR)
		fail("Not yet implemented");
	}

}
