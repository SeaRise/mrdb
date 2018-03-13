package versionmanager;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/*
 * 先完成两阶段加锁,S锁和X锁
 * 用散列表散列数据项(用虚拟地址作为数据项的标识),每一个请求为数据项的链表的表项
 * */
class LockManager {
	private Map<Integer, DataItem> lockMap = new HashMap<Integer, DataItem>();
	
	void lockX(int transactionId, int virtualAddress) {
		if (!lockMap.get(virtualAddress).hasLockX(transactionId)) {
			lock(transactionId, virtualAddress, Lock.lockX);
		}
	}
	
	void lockS(int transactionId, int virtualAddress) {
		if (!lockMap.get(virtualAddress).hasLock(transactionId)) {
			lock(transactionId, virtualAddress, Lock.lockS);
		}
	}
	
	private void lock(int transactionId, int virtualAddress, Lock lock) {
		addDataItem(virtualAddress);
		TranRequest req = new TranRequest(transactionId, lock);
		lockMap.get(virtualAddress).addRequest(req);
	}
	
	void unlock(int transactionId) {
		for (Iterator<Entry<Integer, DataItem>> it = lockMap.entrySet().iterator(); it.hasNext();){
		    Map.Entry<Integer, DataItem> item = it.next();
		    item.getValue().remove(transactionId);
		    if (item.getValue().isEmpty()) {
		    	it.remove();
		    }
		}
	}
	
	boolean canRead(int transactionId, int virtualAddress) {
		if (!lockMap.containsKey(virtualAddress)) {
			return true;
		}
		
		return lockMap.get(virtualAddress).canRead(transactionId);
	}
	
	boolean canWrite(int transactionId, int virtualAddress) {
		if (!lockMap.containsKey(virtualAddress)) {
			return true;
		}
		
		return lockMap.get(virtualAddress).canWrite(transactionId);
	}
	
	boolean upgrade(int transactionId, int virtualAddress) {
		return lockMap.get(virtualAddress).upgrate(transactionId);
	}
	
	private void addDataItem(int virtualAddress) {
		if (!lockMap.containsKey(virtualAddress)) {
			lockMap.put(virtualAddress, new DataItem(virtualAddress));
		}
	}
	
}
