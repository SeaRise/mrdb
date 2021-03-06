package com.mrdb.dm;

import org.junit.Test;

import com.mrdb.dm.exception.OutOfDiskSpaceException;
import com.mrdb.util.pool.BlockPoolExecutor;
import com.mrdb.util.pool.DataBlock;

public class TestDM {
	
	@Test
	public void test() throws OutOfDiskSpaceException {
		DataManager dm = DataManager.getInstance();
		
		int[] addr = new int[200000];
		for (int i = 0; i < 200000; i++) {
			//System.out.println("index" + i);
			DataBlock dataItem = BlockPoolExecutor.getInstance().getDataBlock(4);
			dataItem.writeInt(0, i);
			//System.out.println(dataItem.getInt(0));
			addr[i] = dm.insert(dataItem, 0);
			dataItem.release();
			dataItem = dm.read(addr[i]);
			//System.out.println("len" + dataItem.length);
			System.out.println(dataItem.getInt(0));
			dataItem.release();
			//System.out.println(addr[i]);
		}
		
		System.out.println();
		for (int i = 199999; i >= 0; i--) {
			DataBlock dataItem;
			dataItem = dm.read(addr[i]);
			//System.out.println(dataItem.length);
			System.out.println(dataItem.getInt(0));
			dataItem.release();
			//System.out.println(i);
		}
	}
}
