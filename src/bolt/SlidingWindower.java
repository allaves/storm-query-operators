package bolt;

import java.io.Serializable;

/*
 * Based on https://github.com/nathanmarz/storm-starter/blob/master/src/jvm/storm/starter/tools/SlidingWindowCounter.java
 */
public class SlidingWindower<T> implements Serializable {

	private static final long serialVersionUID = 1225724131483953910L;
	
	private SlotBasedCounter<Graph> graphCounter;
	private int headSlot;
	private int tailSlot;
	private int windowLengthInSlots;

}
