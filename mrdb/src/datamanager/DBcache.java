package datamanager;

class DBcache {
	
	private Page[] cache = new Page[DMSetting.FRAME_NUM];
	
	private DB db = DB.getInstance();
	
	int usedNum = 0;
	
	private static DBcache dbcache = new DBcache();
	
	static DBcache getInstance() {
		return dbcache;
	}
	
	private DBcache() {
		
	}
	
	boolean isFull() {
		return usedNum == DMSetting.FRAME_NUM;
	}
	
	//用于!isFull()
	int addPage(int pageIndex) {
		cache[usedNum] =  db.readPage(pageIndex);
		return usedNum++;
	}
	
	//用于isFull()
	void replacePage(int pageIndex, int pageFrameNo) {
		cache[pageFrameNo] = db.readPage(pageIndex);
	}
	
	void writeBackToDick(int pageFrameNo, int pageIndex) {
		db.updatePage(cache[pageFrameNo], pageIndex);
	}
	
	byte[] read(int physicalAddress) {
		Page page = cache[getPageFrameNo(physicalAddress)];
		int offset = getOffset(physicalAddress);
		return page.getDataItem(offset);
	}
	
	void update(int physicalAddress, byte[] dataItem) {
		Page page = cache[getPageFrameNo(physicalAddress)];
		int offset = getOffset(physicalAddress);
		
		page.setDataItem(offset, dataItem);
	}
	
	private int getPageFrameNo(int physicalAddress) {
		return (physicalAddress >> DMSetting.CHANGE_OFFSET);
	}
	
	private int getOffset(int physicalAddress) {
		return physicalAddress & ((1<<DMSetting.CHANGE_OFFSET)-1);
	}
}
