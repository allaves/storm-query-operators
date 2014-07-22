package state;

import java.util.List;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseStateUpdater;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

public class UpdateJoinState extends BaseStateUpdater<MemoryMapState<TridentTuple>> {

	private static final long serialVersionUID = -342308816758751242L;

	@Override
	public void updateState(MemoryMapState<TridentTuple> state,	List<TridentTuple> tuples, TridentCollector collector) {
		// TODO Auto-generated method stub
		
	}

	


	

}
