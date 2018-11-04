package com.mrdb.dm.page;

import java.io.IOException;
import java.io.RandomAccessFile;

import com.mrdb.dm.DMSetting;

/*存储底层数据
 * 包括索引,页管理,数据
 * */
public class DB {
	
	private RandomAccessFile dbFile = null;
	
	private static DB db = new DB();
	
	private DB() {
		try {
			dbFile = new RandomAccessFile(DMSetting.dbFileName, "rw");
			if (dbFile.length() == 0) {
				initFile(dbFile);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	//第0页会存储一些用于管理数据库的信息,进行初始化
	private void initFile(RandomAccessFile file) {
		try {
			file.setLength(DMSetting.PAGE_NUM*DMSetting.FRAME_SIZE);
			file.seek(0);
			//页管理,存储每个页的已用空间(字节)
			//第一页被用了一些空间
			file.writeInt(4);
			file.writeInt(DMSetting.PAGE_NUM*8);
			for (int i = 1; i < DMSetting.PAGE_NUM; i++) {
				file.writeInt(4);
				file.writeInt(0);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void closeDB() {
		try {
			dbFile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		dbFile = null;
	}
	
	public static DB getInstance() {
		return db;
	}
	
	static void close() {
		db.closeDB();
		db = null;
	}
	
	public void updatePage(Page page, int pageIndex) {
		long address = pageIndex << DMSetting.CHANGE_OFFSET;
		try {
			dbFile.seek(address);
			dbFile.write(page.getContent());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public Page readPage(int pageIndex, Page page) {
		long address = pageIndex << DMSetting.CHANGE_OFFSET;
		try {
			dbFile.seek(address);
			dbFile.readFully(page.getContent());
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return page;
	}
}
