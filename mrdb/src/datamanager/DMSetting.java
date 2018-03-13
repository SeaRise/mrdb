package datamanager;

import util.ParentPath;

public class DMSetting {
	
	static final int PAGE_NUM_INDEX = 5;//指数
	static final int PAGE_NUM = 1 << PAGE_NUM_INDEX; //32
	
	static final int FRAME_NUM_INDEX = 4;//指数
	static final int FRAME_NUM = 1 << FRAME_NUM_INDEX;//16
	
	static final int CHANGE_OFFSET = 13;
	static final int FRAME_SIZE = 1 << CHANGE_OFFSET;//8192;//1<<13
	
	public static final int IM_LOG_NUM = 5;
	
	//用于页面控制存储的第一个地址
	static final int PM_FIRST_ADDRESS = 0;

	static final String dbFileName = ParentPath.dataFileParentPath+"dbFile";
	
	static final String logFileName = ParentPath.dataFileParentPath+"logFile";
}
