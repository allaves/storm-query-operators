package utils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import storm.starter.tools.SlotBasedCounter;

/*
 * Based on https://github.com/nathanmarz/storm-starter/blob/master/src/jvm/storm/starter/tools/SlidingWindowCounter.java
 */
public class SlidingWindow<T> implements Serializable {
	private static final long serialVersionUID = 8321561491740777230L;
	
	private SlotBasedStore<T> store;
	private int headSlot;
	private int tailSlot;
	private int windowLengthInSlots;
	
	public SlidingWindow(int windowLengthInSlots) {
	    if (windowLengthInSlots < 2) {
	      throw new IllegalArgumentException(
	          "Window length in slots must be at least two (you requested " + windowLengthInSlots + ")");
	    }
	    this.windowLengthInSlots = windowLengthInSlots;
	    //this.objCounter = new SlotBasedCounter<T>(this.windowLengthInSlots);
	    this.store = new SlotBasedStore<T>(this.windowLengthInSlots);
	
	    this.headSlot = 0;
	    this.tailSlot = slotAfter(headSlot);
	}
	
//	public void incrementCount(T obj) {
//	    objCounter.incrementCount(obj, headSlot);
//	}
	
	public void addTuple(T obj) {
	    store.addToWindow(obj, headSlot);
	}
	
	
	public Map<Integer, List<T>> getSlotsThenAdvanceWindow() {
		Map<Integer, List<T>> result = store.getSlots();
		store.wipeSlot(tailSlot);
		advanceHead();
		return result;
	}
	
//	  public Map<T, Long> getCountsThenAdvanceWindow() {
//	    Map<Integer, List<T>> slots = store.getSlots();
//	    store.wipeSlot(tailSlot);
//	    advanceHead();
//	    return slots;
//	  }
	
	  private void advanceHead() {
	    headSlot = tailSlot;
	    tailSlot = slotAfter(tailSlot);
	  }
	
	  private int slotAfter(int slot) {
	    return (slot + 1) % windowLengthInSlots;
	  }

}
