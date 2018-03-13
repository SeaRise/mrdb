package versionmanager;

import java.util.Iterator;
import java.util.LinkedList;

/*一个事务在一个数据项上只有一个锁
 * */
class DataItem {
	
	private int virtualAddress;
	
	private LinkedList<TranRequest> reqList = new LinkedList<TranRequest>();
	
	DataItem(int virtualAddress) {
		this.virtualAddress = virtualAddress;
	}
	
	void addRequest(TranRequest req) {
		reqList.addLast(req);
	}
	
	void remove() {
		reqList.removeFirst();
	}
	
	boolean hasLockX(int transactionId) {
		Iterator<TranRequest> iter = reqList.iterator();
		while (iter.hasNext()) {
			TranRequest req = iter.next();
			if (req.transactionId == transactionId && 
					req.getLock().equals(Lock.lockX)) {
				return true;
			}
		}
		return false;
	}
	
	boolean hasLock(int transactionId) {
		Iterator<TranRequest> iter = reqList.iterator();
		while (iter.hasNext()) {
			if (iter.next().transactionId == transactionId) {
				return true;
			}
		}
		
		return false;
	}
	
	void remove(int transactionId) {
		Iterator<TranRequest> iter = reqList.iterator();
		while (iter.hasNext()) {
			if (iter.next().transactionId == transactionId) {
				iter.remove();
				break;
			}
		}
	}
	
	boolean isEmpty() {
		return reqList.isEmpty();
	}
	
	boolean upgrate(int transactionId) {
		/*
		for (TranRequest req : reqList) {
			if (req.transactionId == transactionId) {
				req.upgrate();
				return true;
			}
		}
		
		return false;*/
		if (reqList.isEmpty()) {
			return false;
		}
		
		reqList.getFirst().upgrate();
		return true;
	}
	
	/**
	 * 如果一个数据项被加了X锁, 那么当前被加锁的有且仅有一个且为X锁
	 * 若果一个数据项被加了S锁, 那么可能被加了多个S锁
	 * 若想加X锁,则数据项不能被其他事务加任何锁
	 * 若想加S锁,则数据项不能被其他事务加X锁
	 * */
	boolean canRead(int transactionId) {
		if (reqList.isEmpty()) {
			return true;
		}
		TranRequest req = reqList.getFirst();
		return !req.getLock().equals(Lock.lockX) || 
				req.transactionId == transactionId;
	}
	
	boolean canWrite(int transactionId) {
		if (reqList.isEmpty()) {
			return true;
		}
		TranRequest req = reqList.getFirst();
		return (reqList.size() == 1 && 
				req.transactionId == transactionId &&
				req.equals(Lock.lockS));
	}
	
}
