package datamanager;

import util.DataUtil;

class Test {
	
	public static void main(String[] args) throws OutOfDiskSpaceException {
		
		DataManager dm = DataManager.getInstance();
		
		int[] addr = new int[1900];
		for (int i = 0; i < 1900; i++) {
			byte[] dataItem = new byte[132];
			DataUtil.stringToBytes(i + "", 0, dataItem);
			addr[i] = dm.insert(dataItem, 0);
			dataItem = dm.read(addr[i]);
			System.out.println(dataItem.length);
			System.out.println(DataUtil.bytesToString(dataItem, 0));
			//System.out.println(i);
		}
		System.out.println();
		for (int i = 1899; i >= 0; i--) {
			byte[] dataItem = new byte[132];
			dataItem = dm.read(addr[i]);
			System.out.println(dataItem.length);
			System.out.println(DataUtil.bytesToString(dataItem, 0));
			//System.out.println(i);
		}
		
	}
	
}
