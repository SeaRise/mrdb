package versionmanager;

import java.util.HashMap;
import java.util.Map;

import datamanager.OutOfDiskSpaceException;

class VersionMap {
	
	private Map<Integer, VersionList> map = new HashMap<Integer, VersionList>();
	
	int insert(int firstVersion) throws OutOfDiskSpaceException {
		VersionList list = new VersionList();
		int address = list.init(firstVersion);
		map.put(list.address, list);
		return address;
	}
}
