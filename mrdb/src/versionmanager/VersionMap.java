package versionmanager;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import util.Entry;
import util.pool.DataBlock;
import datamanager.DataManager;
import datamanager.OutOfDiskSpaceException;

class VersionMap {
	
	private static VersionMap vmap = new VersionMap();
	
	private DataManager dm = DataManager.getInstance();
	
	static VersionMap getInstance() {
		return vmap;
	}
	
	private Map<Integer, VersionList> map = new ConcurrentHashMap<Integer, VersionList>();
	
	
	private VersionMap() {}
	
	int insert(int firstVersion) throws OutOfDiskSpaceException {
		VersionList list = new VersionList();
		int address = list.init(firstVersion);
		map.put(list.address, list);
		return address;
	}
	
	private synchronized void loadVersionList(int address) {
		if (!map.containsKey(address)) {
			VersionList v = new VersionList(address);
			v.load();
			map.put(address, v);
		}
	}
	
	DataBlock read(int address, int xid) throws IOException {
		loadVersionList(address);
		VersionList vl = map.get(address);
		
		synchronized (vl) {
			for (Integer v : vl.list) {
				Entry e = new Entry(dm.read(v), true);
				if (Visibility.IsVisible(xid, e)) {
					return e.db;
				}
				e.db.release();
			}
			return null;
		}
	}
	
	void delete(int address, int xid) throws IOException {
		loadVersionList(address);
		VersionList vl = map.get(address);
		
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
	
	void update(int address, int xid, DataBlock db) throws IOException, OutOfDiskSpaceException {
		loadVersionList(address);
		VersionList vl = map.get(address);
		
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
				vl.addVersion(dm.insert(e.db, xid));
			}
		}
	}
}
