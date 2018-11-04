package com.mrdb.dm;

import com.mrdb.util.ParentPath;

public class DMSetting {
	
	public static final int PAGE_NUM_INDEX = 5;//指数
	public static final int PAGE_NUM = 1 << PAGE_NUM_INDEX; //32
	
	public static final int FRAME_NUM_INDEX = 4;//指数
	public static final int FRAME_NUM = 1 << FRAME_NUM_INDEX;//16
	
	public static final int CHANGE_OFFSET = 13;
	public static final int FRAME_SIZE = 1 << CHANGE_OFFSET;//8192;//1<<13
	
	public static final int IM_LOG_NUM = 5;
	
	//用于页面控制存储的第一个地址
	public static final int PM_FIRST_ADDRESS = 0;

	public static final String dbFileName = ParentPath.dataFileParentPath+"dbFile";
	
	public static final String logFileName = ParentPath.dataFileParentPath+"logFile";
}
