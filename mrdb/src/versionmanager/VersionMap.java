package versionmanager;

import java.util.HashMap;
import java.util.Map;

import datamanager.OutOfDiskSpaceException;

class VersionMap {
	
	private static VersionMap vmap = new VersionMap();
	
	static VersionMap getInstance() {
		return vmap;
	}
	
	private Map<Integer, VersionList> map = new HashMap<Integer, VersionList>();
	
	
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
	
	Version read(int address) {
		loadVersionList(address);
		VersionList vl = map.get(address);
		for (Version v : vl.list) {
			v.lockS();
			if (true) {
				return v;
			} else {
				v.unlockS();
			}
		}
		return null;
	}
}
