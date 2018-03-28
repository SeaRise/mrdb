package versionmanager;

import java.io.IOException;

import transactionManager.TransactionManager;
import util.pool.DataBlock;
import datamanager.DataManager;
import datamanager.OutOfDiskSpaceException;

public class VersionManager {
	
	private VersionMap vmap = VersionMap.getInstance();
	
	private DataManager dm = DataManager.getInstance();
	
	private TransactionManager tm = TransactionManager.getInstance();
	
	private static VersionManager vm = new VersionManager();
	
	static public VersionManager getInstance() {
		return vm;
	}
	
	private VersionManager() {
	}
	
	public void startTransaction() throws IOException {
		dm.start(tm.start());
	}
	
	public void abortTransaction() throws IOException {
		dm.abort(tm.getXID());
		tm.abort();
	}
	
	public void commitTransaction() throws IOException {
		dm.commit(tm.getXID());
		tm.commit();
	}
	
	public DataBlock read(int virtualAddress) {
		return dm.read(virtualAddress);
	}
	
	public int insert(DataBlock dataItem, boolean isTransaction) throws OutOfDiskSpaceException {
		return isTransaction ? transactionInsert(dataItem) : onlyInsert(dataItem);
	}
	
	private int onlyInsert(DataBlock dataItem) throws OutOfDiskSpaceException {
		return vmap.insert(dm.insert(dataItem, TransactionManager.SUPER_ID));
	}
	
	private int transactionInsert(DataBlock dataItem) throws OutOfDiskSpaceException {
		return vmap.insert(dm.insert(dataItem, tm.getXID()));
	}
	
	public void update(int virtualAddress, DataBlock dataItem, boolean isTransaction) {
		if (isTransaction) {
			transactionUpdate(virtualAddress, dataItem);
		} else {
			onlyUpdate(virtualAddress, dataItem);
		}
	}
	
	private void onlyUpdate(int virtualAddress, DataBlock dataItem) {
		dm.update(virtualAddress, dataItem, TransactionManager.SUPER_ID);
	}
	
	private void transactionUpdate(int virtualAddress, DataBlock dataItem) {
		dm.update(virtualAddress, dataItem, tm.getXID());
	}
}
