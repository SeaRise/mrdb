package indexmanager.current;

import java.util.HashMap;
import java.util.Map;

public class LockTable {
	
	private static LockTable lt = new LockTable();
	
	private Map<Integer, IndexItem> lockMap = new HashMap<Integer, IndexItem>();
	
	public static LockTable getInstance() {
		return lt;
	}
	
	private LockTable() {
		//new Thread(new ListeningThread()).start();
	}
	
	private class ListeningThread implements Runnable {

		@Override
		public void run() {
			while (true) {
				try {
					Thread.sleep(100000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				synchronized (LockTable.this) {
					
				}
			}
		}
		
	}
	
	synchronized private IndexItem findItem(int address) {
		IndexItem it = lockMap.get(address);
		if (it == null) {
			lockMap.put(address, (it = new IndexItem(address)));
		}
		return it;
	}
	
	public void lockS(int address) {
		findItem(address).lockS();
	}
	
	public void lockX(int address) {
		findItem(address).lockX();
	}
	
	public void unlockS(int address) {
		findItem(address).unlockS();
	}
	
	public void unlockX(int address) {
		findItem(address).unlockX();
	}
	
	public void update(int address) {
		findItem(address).update();
	}
	
	public void degrade(int address) {
		findItem(address).degrade();
	}
}
