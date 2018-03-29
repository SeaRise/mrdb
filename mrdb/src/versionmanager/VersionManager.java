package versionmanager;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import transactionManager.TransactionManager;
import util.Entry;
import util.pool.DataBlock;
import datamanager.DataManager;
import datamanager.OutOfDiskSpaceException;

public class VersionManager {
	
	private VersionMap vmap = VersionMap.getInstance();
	
	private DataManager dm = DataManager.getInstance();
	
	private TransactionManager tm = TransactionManager.getInstance();
	
	private static VersionManager vm = new VersionManager();
	
	ThreadLocal<Integer> level = new ThreadLocal<Integer>();
	
	private HashSet<Integer> activeTran = new HashSet<Integer>();
	
	ThreadLocal<Set<Integer>> activeSnapShot = new ThreadLocal<Set<Integer>>();
	
	static public VersionManager getInstance() {
		return vm;
	}
	
	private VersionManager() {
	}
	
	public void upgradeLevel() {
		level.set(1);
	}
	
	public void degradeLevel() {
		level.set(0);
	}
	
	@SuppressWarnings("unchecked")
	private synchronized void addXid(int xid) {
		//快照
		activeSnapShot.set((Set<Integer>) activeTran.clone());
		activeTran.add(xid);
	}
	
	private synchronized void removeXid(int xid) {
		activeTran.remove(xid);
	}
	
	public void startTransaction() throws IOException {
		int xid = tm.start();
		addXid(xid);
		dm.start(xid);
	}
	
	public void abortTransaction() throws IOException {
		int xid = tm.getXID();
		removeXid(xid);
		dm.abort(xid);
		tm.abort();
	}
	
	public void commitTransaction() throws IOException {
		int xid = tm.getXID();
		removeXid(xid);
		dm.commit(xid);
		tm.commit();
	}
	
	public DataBlock read(int virtualAddress) throws IOException {
		return vmap.read(virtualAddress, tm.getXID());
	}
	
	public int insert(DataBlock dataItem) throws OutOfDiskSpaceException {
		Entry e = new Entry(dataItem, false);
		int xid = tm.getXID();
		e.setXmin(xid);
		return vmap.insert(dm.insert(e.db, xid));
	}
	
	public void update(int virtualAddress, DataBlock dataItem) throws IOException, OutOfDiskSpaceException {
		vmap.update(virtualAddress, tm.getXID(), dataItem);
	}
	
	public void delete(int virtualAddress) throws IOException {
		vmap.delete(virtualAddress, tm.getXID());
	}
}
