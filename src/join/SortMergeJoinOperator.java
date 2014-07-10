package join;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.google.common.base.Predicate;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;

/*
 * Phase 1: sorting R and S by join attributes. Elimination of not needed records.
 * Phase 2: Iteration over R and S, return results in case of matching join condition.
 * 
 * Complexity: O(N*log(M))
 */
public class SortMergeJoinOperator {
	
	
	public SortMergeJoinOperator() {
		
	}
	
	
	/*
	 * Sort-Merge Join
	 */
	public LinkedHashMap<String, Tuple> execute(LinkedHashMap<String, Tuple> s, LinkedHashMap<String, Tuple> r, String joinAttribute, Predicate<Tuple> conditionS, Predicate<Tuple> conditionR) throws SQLException {
		// Phase 1: Sort
		TreeMap<String, Tuple> sortedS = sort(s, joinAttribute, conditionS);
		TreeMap<String, Tuple> sortedR = sort(r, joinAttribute, conditionR);
		// Phase 2: Merge
		return merge(sortedS, sortedR, joinAttribute);
	}

	
	/*
	 * Inserts all entries of a LinkedHashMap into a TreeMap.
	 * Cost: O(n*log n) ?
	 */
	@SuppressWarnings("unused")
	private TreeMap<String, Tuple> sortAll(LinkedHashMap<String, Tuple> table, String joinAttribute) {
		TreeMap<String, Tuple> treeMap = new TreeMap<String, Tuple>();
		treeMap.putAll(table);
		return treeMap;
	}
	
	
	/*
	 * Inserts all entries of a LinkedHashMap, one by one, into a TreeMap. Eliminates records that do not pass the predicate evaluation.
	 * (Guava) predicates are defined w.r.t. a Storm Tuple and have to be implemented by the join operator caller.
	 */
	private TreeMap<String, Tuple> sort(LinkedHashMap<String, Tuple> table, String joinAttribute, Predicate<Tuple> condition) {
		TreeMap<String, Tuple> treeMap = new TreeMap<String, Tuple>();
		for (Entry<String, Tuple> entry: table.entrySet()) {
			if (condition.apply(entry.getValue())) {
				treeMap.put(entry.getKey(), entry.getValue());
			}
		}
		return treeMap;
	}


	/*
	 * Phase 2: Iteration over R and S, return results in case of matching join condition.
	 */
	private LinkedHashMap<String, Tuple> merge(TreeMap<String, Tuple> sortedS, TreeMap<String, Tuple> sortedR, String joinAttribute) {
		// Are LinkedHashMap and LinkedList the best data structures for the result set and the intermediate list?
		LinkedHashMap<String, Tuple> res = new LinkedHashMap<String, Tuple>();
		LinkedList<Object> listRes = null;
		Tuple auxS, auxR = null;
		// Iterate over sortedS
		for (Entry<String, Tuple> entryS : sortedS.entrySet()) {
			auxS = entryS.getValue();
			// Find record in sortedR with key from entryS
			if (sortedR.containsKey(entryS.getKey())) {
				auxR = sortedR.get(entryS.getKey());
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
		return res;
	}
	
	
	/*
	 * Phase 2: Iteration over R and S, return results in case of matching join condition.
	 * Interleaved scans with while loop
	 */
	private LinkedHashMap<String, Tuple> interleavedMerge(TreeMap<String, Tuple> sortedS, TreeMap<String, Tuple> sortedR, String joinAttribute) {
		// Are LinkedHashMap and LinkedList the best data structures for the result set and the intermediate list?
		LinkedHashMap<String, Tuple> res = new LinkedHashMap<String, Tuple>();
		LinkedList<Object> listRes = null;
		Tuple auxS, auxR = null;
		int comparation;
		// Interleaved iteration over sortedS and sortedR
		for (Entry<String, Tuple> entryS : sortedS.entrySet()) {
			auxS = entryS.getValue();
			for (Entry<String, Tuple> entryR : sortedS.entrySet()) {
				auxR = entryR.getValue();
				// Compares the JA of the two tables
				comparation = auxS.getStringByField(joinAttribute).compareTo(auxR.getStringByField(joinAttribute));
				// if entryS JA is lex. greater than entryR JA, the iterator of for loop entryR advances
				if (comparation > 0) {
					continue;
				}
				// if entryS.JA is lex. lower than entryR.JA, the iterator of for loop entryS advances
				else if (comparation < 0) {
					break;
				}
				// if entryS.JA == entryR.JA, both tuples are combined and included in the results
				else if (comparation == 0) {
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
