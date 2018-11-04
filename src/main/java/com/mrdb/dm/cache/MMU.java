package com.mrdb.dm.cache;

import com.mrdb.dm.DMSetting;
import com.mrdb.dm.exception.VirtualAddressOutOfLimitException;

public class MMU {
	
	private PageItem[] mmuTable = new PageItem[DMSetting.PAGE_NUM];
	
	private DBcache cache = DBcache.getInstance();
	
	static private MMU mmu = new MMU();
	
	public static MMU getInstance() {
		return mmu;
	}
	
	private MMU() {
		for (int i = 0; i < DMSetting.PAGE_NUM; i++) {
			mmuTable[i] = new PageItem();
		}
	}
	
	class PageItem {
		int pageFrameNo = -1; //页框号
		boolean stay = false;//是否在
		boolean modified = false; // 修改位
		int R = 1<<30; //访问位,老化算法,用于置换
		
		//恢复初始状态
		void reset() {
			pageFrameNo = -1; 
			stay = false;
			modified = false; 
			R = 1<<30; 
		}
		
		void use(int pageFrameNo) {
			this.pageFrameNo = pageFrameNo;
			stay = true;
			R = 1<<30;
		}
		
		void beVisited() {
			R += 1<<30;
		}
		
		void modify() {
			modified = true;
		}
	}
	
	
	/**
	 * 将虚拟地址转换为物理地址:页框号+偏移量
	 * @throws VirtualAddressOutOfLimitException 
	 * */
	public int changeVirtualAddressToPhysicalAddress(int virtualAddress) {
		updateR();
		if (virtualAddress >= (1<<(DMSetting.PAGE_NUM_INDEX + DMSetting.CHANGE_OFFSET)) || virtualAddress < 0) {
			try {
				throw new VirtualAddressOutOfLimitException();
			} catch (VirtualAddressOutOfLimitException e) {
				e.printStackTrace();
			}
		}
		
		int pageIndex = (virtualAddress >> DMSetting.CHANGE_OFFSET); //页号
		PageItem item = mmuTable[pageIndex];
		if (!item.stay) {//发生缺页
			loadPage(pageIndex);//把对应页载入页框
		} else {
			item.beVisited();
		}
		
	    return (item.pageFrameNo << DMSetting.CHANGE_OFFSET) + 
	    		(virtualAddress & ((1<<DMSetting.CHANGE_OFFSET)-1)); //页框号+偏移量
	}
	
	/**缺页中断处理
	 * 装载页面
	 * */
	private void loadPage(int pageIndex) {
		if (!cache.isFull()) {
			//页框未用完
			mmuTable[pageIndex].use(cache.addPage(pageIndex));
		} else {
			//页框用完
			pageReplacement(pageIndex);
		}
	}
	
	private void pageReplacement(int pageIndex) {
		int minIndex = -1;
		short minR = Short.MAX_VALUE;
		for (int i = 0; i < mmuTable.length; i++) {
			if (mmuTable[i].stay && mmuTable[i].R < minR) {
				minIndex = i;
			}
		}
		
		if (mmuTable[minIndex].modified) {
			cache.writeBackToDick(mmuTable[minIndex].pageFrameNo, minIndex);
		}
		replacePage(pageIndex, mmuTable[minIndex].pageFrameNo);
		mmuTable[minIndex].reset();//取消页框的使用权,恢复初始状态
	}
	
	void flush() {
		for (int i = 0; i < mmuTable.length; i++) {
			if (mmuTable[i].stay && mmuTable[i].modified) {
				cache.writeBackToDick(mmuTable[i].pageFrameNo, i);
				mmuTable[i].modified = false;
			}
		}
	}
	
	public void modifyPage(int virtualAddress) {
		int pageIndex = (virtualAddress >> DMSetting.CHANGE_OFFSET);
		mmuTable[pageIndex].modify();
	}
	
	//替换时
	private void replacePage(int pageIndex, int pageFrameNo) {
		cache.replacePage(pageIndex, pageFrameNo);
		mmuTable[pageIndex].use(pageFrameNo);
	}
	
	//每一次操作都会触发一次这个方法,mmu采用被动式更新R值
	private void updateR() {
		for (PageItem item : mmuTable) {
			if (item.stay) {
				item.R >>= 1;
			}
		}
	}
}
