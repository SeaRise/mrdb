package com.mrdb.vm;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.mrdb.dm.DataManager;
import com.mrdb.dm.exception.OutOfDiskSpaceException;
import com.mrdb.util.Entry;
import com.mrdb.util.pool.DataBlock;

class VersionMap {
	
	private static VersionMap vmap = new VersionMap();
	
	private DataManager dm = DataManager.getInstance();
	
	// Logger
	private final static Logger LOGGER = Logger.getLogger(VersionMap.class.getName());
	
	static VersionMap getInstance() {
		return vmap;
	}
	
	private Map<Integer, VersionList> map = new HashMap<Integer, VersionList>();
	
	private VersionMap() {}
	
	synchronized int insert(int firstVersion) {
		VersionList list = new VersionList();
		int address = -1;
		try {
			address = list.init(firstVersion);
		} catch (OutOfDiskSpaceException e) {
			LOGGER.log(Level.INFO, "插入数据失败,dm空间不足, address:" + address);
		}
		//LOGGER.log(Level.INFO, "插入数据," + "vl len" + list.list.size() + " address " + address);
		map.put(list.address, list);
		return address;
	}
	
	private void loadVersionList(int address) {
		if (!map.containsKey(address)) {
			VersionList v = new VersionList(address);
			v.load();
			//LOGGER.log(Level.INFO, "读入版本链数据  " + " vl len " + v.list.size());
			map.put(address, v);
		}
	}
	
	DataBlock read(int address, int xid) {
		VersionList vl;
		synchronized (this) {
			loadVersionList(address);
			vl = map.get(address);
		}
		synchronized (vl) {
			//LOGGER.log(Level.INFO, "开始读取数据,xid = " + xid + " vl len " + vl.list.size() + " address " + address);
			for (Integer v : vl.list) {
				Entry e = new Entry(dm.read(v), true);
				if (Visibility.IsVisible(xid, e)) {
					return e.db;
				}
				e.db.release();
			}
			//LOGGER.log(Level.INFO, "读取无数据,xid = " + xid);
			return null;
		}
	}
	
	void delete(int address, int xid) {
		VersionList vl;
		synchronized (this) {
			loadVersionList(address);
			vl = map.get(address);
		}
		synchronized (vl) {
			Entry e = null;
			int deleteAddress = -1;
			for (Integer v : vl.list) {
				e = new Entry(dm.read(v), true);
				if (Visibility.IsVisible(xid, e)) {
					deleteAddress = v;
					break;
				}
				e.db.release();
			}
			if (deleteAddress != -1) {
				e.setXmax(xid);
				dm.update(deleteAddress, e.db, xid);
				e.db.release();
			}
		}
	}
	
	void update(int address, int xid, DataBlock db) {
		VersionList vl;
		synchronized (this) {
			loadVersionList(address);
			vl = map.get(address);
		}
		synchronized (vl) {
			Entry e = null;
			int updateAddress = -1;
			for (Integer v : vl.list) {
				e = new Entry(dm.read(v), true);
				if (Visibility.IsVisible(xid, e)) {
					updateAddress = v;
					break;
				}
				e.db.release();
			}
			if (updateAddress != -1) {
				e.setXmax(xid);
				dm.update(updateAddress, e.db, xid);
				e.db.release();
				
				e = new Entry(db, false);
				e.setXmin(xid);
				//这时的e.db用户还持有,由用户来释放.
				try {
					vl.addVersion(dm.insert(e.db, xid));
				} catch (OutOfDiskSpaceException e1) {
					LOGGER.log(Level.INFO, "写入一个新版本,dm空间 不足" + " address " + address);
				}
			}
		}
	}
}
