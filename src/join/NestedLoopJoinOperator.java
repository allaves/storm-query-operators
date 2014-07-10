package join;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;

import com.google.common.base.Predicate;

/*
 * Applied when records in tables R and S are not ordered according to join attributes,
 * and there is no presence of indexes on join attributes.
 * 
 * Complexity: O(N*M)
 */
public class NestedLoopJoinOperator {
	
	
	public NestedLoopJoinOperator() {
		
	}
	
	/*
	 * Nested-loop for equity join (no indexes)
	 */
	public static LinkedHashMap<String, Tuple> execute(LinkedList<Tuple> s, LinkedList<Tuple> r, String joinAttribute, Predicate<Tuple> conditionS, Predicate<Tuple> conditionR) {
		// Are LinkedHashMap and LinkedList the best data structures for the result set and the intermediate list?
		LinkedHashMap<String, Tuple> res = new LinkedHashMap<String, Tuple>();
		LinkedList<Object> listRes = null;
		Tuple auxS, auxR = null;
		// Scan over S,
		while ((auxS = s.iterator().next()) != null) {
			// for each record in s, if Ps...
			if (conditionS.apply(auxS)) {
				// ...scan over r,
				while ((auxR = r.iterator().next()) != null) {
					// for each record in r, if Pr AND (r.JA == s.JA):
					if (conditionR.apply(auxR) && (auxS.getStringByField(joinAttribute) == auxR.getStringByField(joinAttribute))) {
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
		}
		
		return res;
	}
	
	

}
