package versionmanager;

import datamanager.DataManager;
import datamanager.OutOfDiskSpaceException;

/*
 * 线程同步问题待解决,暂时采用自旋锁
 * 暂时想到wait,在unlock的时候用notify激活
 * */
public class VersionManager {
	
	private LockManager lm = new LockManager();
	
	private DataManager dm = DataManager.getInstance();
	
	public synchronized int insert(byte[] dataItem, int transactionId) throws OutOfDiskSpaceException {
		return dm.insert(dataItem, transactionId);
	}
	
	public synchronized byte[] read(int transactionId, int virtualAddress) {
		
		while (!lm.canRead(transactionId, virtualAddress)) {
			try {
				wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		lm.lockS(transactionId, virtualAddress);
		
		return dm.read(virtualAddress);
	}
	
	public synchronized void update(int virtualAddress, byte[] dataItem, int transactionId) {
		
		while(!lm.canWrite(transactionId, virtualAddress)) {
			try {
				wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		if (!lm.upgrade(transactionId, virtualAddress)) {
			lm.lockX(transactionId, virtualAddress);
		}
		
		dm.update(virtualAddress, dataItem, transactionId);
	}
	
	public synchronized void start(int transactionId) {
		dm.start(transactionId);
	}
	
	public synchronized void commit(int transactionId) {
		dm.commit(transactionId);
		lm.unlock(transactionId);
	}
	
	public synchronized void abort(int transactionId) {
		dm.abort(transactionId);
		lm.unlock(transactionId);
		notifyAll();
	}
	
}


