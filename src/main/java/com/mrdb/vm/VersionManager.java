package com.mrdb.vm;

import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.mrdb.dm.DataManager;
import com.mrdb.dm.exception.OutOfDiskSpaceException;
import com.mrdb.tm.TransactionManager;
import com.mrdb.util.Entry;
import com.mrdb.util.pool.DataBlock;

public class VersionManager {
	
	private VersionMap vmap = VersionMap.getInstance();
	
	private DataManager dm = DataManager.getInstance();
	
	private TransactionManager tm = TransactionManager.getInstance();
	
	private static VersionManager vm = new VersionManager();
	
	ThreadLocal<Integer> level = new ThreadLocal<Integer>();
	
	private HashSet<Integer> activeTran = new HashSet<Integer>();
	
	ThreadLocal<Set<Integer>> activeSnapShot = new ThreadLocal<Set<Integer>>();
	
	//Logger
    private final static Logger LOGGER = Logger.getLogger(VersionManager.class.getName());
	
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
		//快照,在可重复读情况下
		Integer lev = level.get();
		if (lev != null && lev.equals(1)) {
			activeSnapShot.set((Set<Integer>) activeTran.clone());
		}
		
		activeTran.add(xid);
	}
	
	private synchronized void removeXid(int xid) {
		activeTran.remove(xid);
	}
	
	public void startTransaction() {
		int xid = tm.start();
		//LOGGER.log(Level.INFO, "开始一个事务,xid = " + xid);
		addXid(xid);
		dm.start(xid);
	}
	
	public void abortTransaction() {
		int xid = tm.getXID();
		//LOGGER.log(Level.INFO, "结束一个事务,xid = " + xid);
		removeXid(xid);
		dm.abort(xid);
		tm.abort();
	}
	
	public void commitTransaction() {
		int xid = tm.getXID();
		//LOGGER.log(Level.INFO, "结束一个事务,xid = " + xid);
		removeXid(xid);
		dm.commit(xid);
		tm.commit();
	}
	
	public DataBlock read(int virtualAddress) {
		//LOGGER.log(Level.INFO, "读,xid = " + tm.getXID());
		return vmap.read(virtualAddress, tm.getXID());
	}
	
	public int insert(DataBlock dataItem) {
		Entry e = new Entry(dataItem, false);
		int xid = tm.getXID();
		//LOGGER.log(Level.INFO, "插入,xid = " + xid);
		e.setXmin(xid);
		int address = -1;
		try {
			address = vmap.insert(dm.insert(e.db, xid));
		} catch (OutOfDiskSpaceException e1) {
			LOGGER.log(Level.INFO, "插入数据失败,dm空间不足");
		}
		return address;
	}
	
	public void update(int virtualAddress, DataBlock dataItem) {
		vmap.update(virtualAddress, tm.getXID(), dataItem);
	}
	
	public void delete(int virtualAddress) {
		vmap.delete(virtualAddress, tm.getXID());
	}
}
