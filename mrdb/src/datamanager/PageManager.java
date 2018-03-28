package datamanager;

import transactionManager.TransactionManager;
import util.pool.BlockPoolExecutor;
import util.pool.DataBlock;

/**用于管理每个页分配情况
 * */
class PageManager {
	
	private static PageManager pm = new PageManager();
	
	private DataManager dm = null;
	
	//尾偏移量数组,尾偏移量:最靠前的未使用偏移量
	private int[] lastOffsets = new int[DMSetting.PAGE_NUM];
	
	static PageManager getInstance() {
		return pm;
	}
	
	private PageManager() {
		
	}
	
	void init(DataManager dm) {
		this.dm = dm;
		for (int i = 0; i < DMSetting.PAGE_NUM; i++) {
			lastOffsets[i] = dm.read(DMSetting.PM_FIRST_ADDRESS + i*8).getInt(0);
		}
	}
	
	//返回虚拟地址
	int applyBlock(int len) throws OutOfDiskSpaceException {
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
		DataBlock dataItem = BlockPoolExecutor.getInstance().getDataBlock(4);
		dataItem.writeInt(0, lastOffsets[pageIndex]);
	    dm.update(DMSetting.PM_FIRST_ADDRESS + pageIndex*8, dataItem, TransactionManager.SUPER_ID);
	}
	
	//计算剩余空间
	private int getSurplusSpace(int lastAddress) {
		return DMSetting.FRAME_SIZE-lastAddress;
	}
}
