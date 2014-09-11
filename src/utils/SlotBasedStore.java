package utils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SlotBasedStore<T> implements Serializable {
	
	private static final long serialVersionUID = 5616365865624225395L;
	
	private final Map<Integer, List<T>> graphToWindow = new HashMap<Integer, List<T>>();
	private final int numSlots;
	
	  public SlotBasedStore(int numSlots) {
	    if (numSlots <= 0) {
	      throw new IllegalArgumentException("Number of slots must be greater than zero (you requested " + numSlots + ")");
	    }
	    this.numSlots = numSlots;
	  }
	
	  public void addToWindow(T g, int slot) {
	  	List<T> graphList = graphToWindow.get(slot);
		if (graphList == null) {
			graphList = new ArrayList<T>();
			graphToWindow.put(slot, graphList);
		}
		graphList.add(g);
	  }
	  
	  
	  
//	  public void incrementCount(T obj, int slot) {
//	    long[] counts = objToCounts.get(obj);
//	    if (counts == null) {
//	      counts = new long[this.numSlots];
//	      objToCounts.put(obj, counts);
//	    }
//	    counts[slot]++;
//	  }
	
	  public List<T> getSlot(int slot) {
	    return graphToWindow.get(slot);
	  }
	  
	  public Map<Integer, List<T>> getSlots() {
		  return graphToWindow;
	  }
	  
//	  public long getCount(T obj, int slot) {
//	    long[] counts = objToCounts.get(obj);
//	    if (counts == null) {
//	      return 0;
//	    }
//	    else {
//	      return counts[slot];
//	    }
//	  }
	  
//	  public Map<T, Long> getCounts() {
//	    Map<T, Long> result = new HashMap<T, Long>();
//	    for (T obj : objToCounts.keySet()) {
//	      result.put(obj, computeTotalCount(obj));
//	    }
//	    return result;
//	  }
	
//	  private long computeTotalCount(T obj) {
//	    long[] curr = objToCounts.get(obj);
//	    long total = 0;
//	    for (long l : curr) {
//	      total += l;
//	    }
//	    return total;
//	  }
	
	  /**
	   * Reset the slot count of any tracked objects to zero for the given slot.
	   *
	   * @param slot
	   */
	  public void wipeSlot(int slot) {
		  graphToWindow.put(slot, null);
	  }
	
//	  private void resetSlotCountToZero(T obj, int slot) {
//	    long[] counts = objToCounts.get(obj);
//	    counts[slot] = 0;
//	  }
	
//	  private boolean shouldBeRemovedFromCounter(T obj) {
//	    return computeTotalCount(obj) == 0;
//	  }
	
	  /**
	   * Remove any object from the counter whose total count is zero (to free up memory).
	   */
//	  public void wipeZeros() {
//	    Set<T> objToBeRemoved = new HashSet<T>();
//	    for (T obj : objToCounts.keySet()) {
//	      if (shouldBeRemovedFromCounter(obj)) {
//	        objToBeRemoved.add(obj);
//	      }
//	    }
//	    for (T obj : objToBeRemoved) {
//	      objToCounts.remove(obj);
//	    }
//	  }
	
	}	 
	
	  