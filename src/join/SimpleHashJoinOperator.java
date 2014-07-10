package join;

import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;

import com.google.common.base.Predicate;

/*
 * Step 1: Partition of smaller table R and construction of hash table w.r.t. values of R(JA)
 * of partitions Ri(1 <= i <= p): each partition fits into the available memory and each record satisfies Pr.
 * 
 * Step 2: Probing for records of S using Ps. If successful, execution of JOIN.
 *  
 * Step 3: Repeat steps 1 and 2 as long as R is exhausted.
 * 
 * From http://wwwlgis.informatik.uni-kl.de/cms/fileadmin/courses/SS2011/RDBS/lectures/Chapter_11_Table_Operations.pdf
 */
public class SimpleHashJoinOperator {

	
	public LinkedHashMap<String, Tuple> execute(LinkedHashMap<String, Tuple> s, LinkedHashMap<String, Tuple> r, String joinAttribute, 
			Predicate<Tuple> conditionS, Predicate<Tuple> conditionR) {
		// Identify larger and smaller table. I assume s is smaller.
		if (s.size() >= r.size()) {
			return execute(r, s, joinAttribute, conditionR, conditionS);
		}
		LinkedHashMap<String, Tuple> res = new LinkedHashMap<String, Tuple>();
		LinkedList<Object> listRes = null;
		Tuple auxS, auxR = null;
		// Scan over larger table
		for (Entry<String, Tuple> entryR: r.entrySet()) {
			if (conditionR.apply(entryR.getValue())) {
				auxR = entryR.getValue();
				// Look for rows in smaller table
				auxS = s.get(entryR.getKey());
				// write combined result (r,s) into result set
				// I assume that the remove operation has lower cost with the smaller list
				if (conditionS.apply(auxS)) {
					if (auxR.size() >= auxS.size()) {
						listRes = new LinkedList<Object>(auxR.getValues());
						List<Object> auxList = new LinkedList<Object>(auxR.getValues());
						auxList.remove(auxS.fieldIndex(joinAttribute));
						listRes.addAll(auxList);
					}
					else {
						listRes = new LinkedList<Object>(auxS.getValues());
						List<Object> auxList = new LinkedList<Object>(auxR.getValues());
						auxList.remove(auxR.fieldIndex(joinAttribute));
						listRes.addAll(auxList);
					}
				}
				res.put(joinAttribute, new TupleImpl(null, listRes, 0, null, null));
			}
		}
		return res;
	}
}
