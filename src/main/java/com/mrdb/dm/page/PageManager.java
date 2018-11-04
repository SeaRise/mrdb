package com.mrdb.dm.page;

import com.mrdb.dm.DMSetting;
import com.mrdb.dm.DataManager;
import com.mrdb.dm.exception.OutOfDiskSpaceException;
import com.mrdb.tm.TransactionManager;
import com.mrdb.util.pool.BlockPoolExecutor;
import com.mrdb.util.pool.DataBlock;

/**用于管理每个页分配情况
 * */
public class PageManager {
	
	private static PageManager pm = new PageManager();
	
	private DataManager dm = null;
	
	//尾偏移量数组,尾偏移量:最靠前的未使用偏移量
	private int[] lastOffsets = new int[DMSetting.PAGE_NUM];
	
	public static PageManager getInstance() {
		return pm;
	}
	
	private PageManager() {
		
	}
	
	public void init(DataManager dm) {
		this.dm = dm;
		DataBlock db = null;
		for (int i = 0; i < DMSetting.PAGE_NUM; i++) {
			db = dm.read(DMSetting.PM_FIRST_ADDRESS + i*8);
			lastOffsets[i] = db.getInt(0);
			db.release();
		}
	}
	
	//返回虚拟地址
	public int applyBlock(int len) throws OutOfDiskSpaceException {
		int pageIndex = 0;
		for (; pageIndex < DMSetting.PAGE_NUM; pageIndex++) {
			if (len <= getSurplusSpace(lastOffsets[pageIndex])) {
				break;
			}
		}
		
		//找不到可用的块
		if (pageIndex == DMSetting.PAGE_NUM) {
			throw new OutOfDiskSpaceException();
		}

		int virtualAddress = lastOffsets[pageIndex] + (pageIndex<<DMSetting.CHANGE_OFFSET);
		lastOffsets[pageIndex] += len;
		updatePageInf(pageIndex);
		return virtualAddress;
	}
	
	//更新尾偏移量
	private void updatePageInf(int pageIndex) {
		DataBlock db = BlockPoolExecutor.getInstance().getDataBlock(4);
		db.writeInt(0, lastOffsets[pageIndex]);
	    dm.update(DMSetting.PM_FIRST_ADDRESS + pageIndex*8, db, TransactionManager.SUPER_ID);
	    db.release();
	}
	
	//计算剩余空间
	private int getSurplusSpace(int lastAddress) {
		return DMSetting.FRAME_SIZE-lastAddress;
	}
}
