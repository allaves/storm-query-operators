package state;

import java.util.HashMap;

import storm.trident.state.ValueUpdater;
import storm.trident.state.snapshot.Snapshottable;

public class SensorLocationState implements Snapshottable<String> {

	private HashMap<String, String> map;

	@Override
	public String get() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void beginCommit(Long txid) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void commit(Long txid) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String update(ValueUpdater updater) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void set(String o) {
		// TODO Auto-generated method stub
		
	}
	
	

	
	
}
