package join;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;

import com.google.common.base.Predicate;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;

public class IndexedNestedLoopJoinOperator {
	
	
	public IndexedNestedLoopJoinOperator() {
	}
	
	/*
	 * Nested-loop for equity join (with indexes)
	 * I assume that the joinAttribute is also the row ID in each "table"
	 */
	public LinkedHashMap<String, Tuple> execute(LinkedHashMap<String, Tuple> s, LinkedHashMap<String, Tuple> r, String joinAttribute, Predicate<Tuple> conditionS, Predicate<Tuple> conditionR) {
		// Are LinkedHashMap and LinkedList the best data structures for the result set and the intermediate list?
		LinkedHashMap<String, Tuple> res = new LinkedHashMap<String, Tuple>();
		LinkedList<Object> listRes = null;
		Tuple auxS, auxR = null;
		// Scan over S,
		while ((auxS = s.values().iterator().next()) != null) {
			// for each record in s, if Ps...
			if (conditionS.apply(auxS)) {
				// ...determine via acces to indexR(JA) all records satisfying r.JA == s.JA,
				auxR = r.get(auxS.getValueByField(joinAttribute));
				// for each record in r, if Pr:
				if (conditionS.apply(auxR)) {
					// write combined result (r,s) into result set
					// I assume that the remove operation has lower cost with the smaller list
					if (auxR.size() >= auxS.size()) {
						listRes = new LinkedList<Object>(auxR.getValues());
						List<Object> auxList = new LinkedList<Object>(auxS.getValues());
						auxList.remove(auxS.fieldIndex(joinAttribute));
						listRes.addAll(auxList);
					}
					else {
						listRes = new LinkedList<Object>(auxS.getValues());
						List<Object> auxList = new LinkedList<Object>(auxR.getValues());
						auxList.remove(auxR.fieldIndex(joinAttribute));
						listRes.addAll(auxList);
					}
					res.put(joinAttribute, new TupleImpl(null, listRes, 0, null, null));
				}
			}
		}
		return res;
	}

}
