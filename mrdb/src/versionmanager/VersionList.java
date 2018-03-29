package versionmanager;

import java.util.LinkedList;
import java.util.List;

import transactionManager.TransactionManager;
import util.pool.BlockPoolExecutor;
import util.pool.DataBlock;
import datamanager.DataManager;
import datamanager.OutOfDiskSpaceException;

class VersionList {
	
	private final DataManager dm = DataManager.getInstance();
	
	int address;
	
	private int lastAddress;
	
	static final int PART_LEN = (1+4+1)*4;//,第一个是该段的版本数目,最后一个是下一段的位置,初始话-1
	static final int PART_FIRST_VERSION_POS = 4;
	static final int PART_LAST_POS = 4*4;
	
	final LinkedList<Integer> list = new LinkedList<Integer>();
	
	VersionList() {
		
	}
	
	VersionList(int address) {
		this.address = address;
	}
	
	int init(int firstVersion) throws OutOfDiskSpaceException {
		this.address = doInit(firstVersion);
		this.lastAddress = address;
		return address;
	}
	
	private int doInit(int firstVersion) throws OutOfDiskSpaceException {
		DataBlock db = BlockPoolExecutor.getInstance().getDataBlock(PART_LEN);
		list.addFirst(firstVersion);
		db.writeInt(0, 1);
		db.writeInt(PART_FIRST_VERSION_POS, firstVersion);
		db.writeInt(PART_LAST_POS, -1);
		int addr = dm.insert(db, TransactionManager.SUPER_ID);
		db.release();
		return addr;
	}
	
	void load() {
		int pos = address;
		while (pos != -1) {
			lastAddress = pos;
			DataBlock db = dm.read(address);
			int n = db.getInt(0);
			for (int i = 0; i < n; i++) {
				list.addFirst(db.getInt(PART_FIRST_VERSION_POS+i*4));
			}
			pos = db.getInt(PART_LAST_POS);
			db.release();
		}
	}
	
	void addVersion(int vesionAddr) throws OutOfDiskSpaceException {
		DataBlock db = dm.read(lastAddress);
		int n = db.getInt(0);
		int lastAddr;
		if (n == 4) {
			lastAddr = doInit(vesionAddr);
			db.writeInt(PART_LAST_POS, lastAddr);
		} else {
			lastAddr = lastAddress;
			db.writeInt(PART_FIRST_VERSION_POS + n*4, vesionAddr);
		}
		dm.update(lastAddress, db, TransactionManager.SUPER_ID);
		db.release();
		list.addFirst(vesionAddr);
		lastAddress = lastAddr;
	}
}
