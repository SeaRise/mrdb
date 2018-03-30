package indexmanager.current;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.Set;

public class LockTable {
	
	private static LockTable lt = new LockTable(500);
	
	private Map<Integer, IndexItem> lockMap = new HashMap<Integer, IndexItem>();
	
	private final ReentrantReadWriteLock rwlock = new ReentrantReadWriteLock();
	
	public static LockTable getInstance() {
		return lt;
	}
	
	private LockTable(long millis) {
		//new Thread(new ListeningThread(millis)).start();
	}
	
	//监听线程
	private class ListeningThread implements Runnable {

		final long millis;
		
		ListeningThread(long millis) {
			this.millis = millis;
		}
		
		@Override
		public void run() {
			while (true) {
				try {
					Thread.sleep(millis);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				rwlock.writeLock().lock();
				//清理无用项
				Set<Entry<Integer, IndexItem>> set = lockMap.entrySet();
				Iterator<Entry<Integer, IndexItem>> it = set.iterator();
				while (it.hasNext()) {
					IndexItem item = it.next().getValue();
					if (item.canClear()) {
						it.remove();
						lockMap.remove(item.address);
					}
				}	
				rwlock.writeLock().unlock();
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
		rwlock.readLock().lock();
		findItem(address).lockS();
		rwlock.readLock().unlock();
	}
	
	public void lockX(int address) {
		rwlock.readLock().lock();
		findItem(address).lockX();
		rwlock.readLock().unlock();
	}
	
	public void unlockS(int address) {
		rwlock.readLock().lock();
		findItem(address).unlockS();
		rwlock.readLock().unlock();
	}
	
	public void unlockX(int address) {
		rwlock.readLock().lock();
		findItem(address).unlockX();
		rwlock.readLock().unlock();
	}
}
