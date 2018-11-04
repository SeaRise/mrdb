package com.mrdb.dm.log;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayDeque;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.mrdb.dm.DMSetting;
import com.mrdb.dm.cache.DBcache;
import com.mrdb.dm.cache.MMU;
import com.mrdb.tm.TransactionManager;
import com.mrdb.tm.TransactionType;
import com.mrdb.util.pool.DataBlock;

public class LogFileManager {
	
	// Logger
	private final static Logger LOGGER = Logger.getLogger(LogFileManager.class.getName());
	
	private RandomAccessFile logFile = null;
	
	private DBcache cache = DBcache.getInstance();
	
	private MMU mmu = MMU.getInstance();
	
	static private LogFileManager lfm = new LogFileManager();
	
	private TransactionManager tm = TransactionManager.getInstance();
	
	private ArrayDeque<Integer> undoList = new ArrayDeque<Integer>();
	
	public static LogFileManager getInstance() {
		return lfm;
	}
	
	private LogFileManager() {
		try {
			logFile = new RandomAccessFile(DMSetting.logFileName, "rw");
		} catch (FileNotFoundException e) {
			LOGGER.log(Level.INFO, "日志文件生成失败:" + DMSetting.logFileName);
		}
	}
	
	public void login(int transactionId, TransactionType type, int virtualAddress, 
			DataBlock oldItem, DataBlock newItem) {
		try {
			logFile.seek(logFile.length());
			logFile.writeInt(transactionId);
			type.writeToFile(logFile);
			logFile.writeInt(virtualAddress);
			//old block
			if (oldItem == null) {
				logFile.writeInt(0);
			} else {
				logFile.writeInt(oldItem.length);
				oldItem.writeToFile(logFile);
			}
			//new block
			if (newItem == null) {
				logFile.writeInt(0);
			} else {
				logFile.writeInt(newItem.length);
				newItem.writeToFile(logFile);
			}
			
			//Unit大小这个量的大小这个不需要,因为不用倒着读.
			//计算出Unit的大小,便于倒着读文件
			/*
			logFile.writeInt(4+2+4+ // transactionId+type+virtualAddress
					4+(oldItem == null ? 0 : oldItem.length)+ // oldItem.length+oldItem
					4+(newItem == null ? 0 : newItem.length)+ // newItem.length+newItem
					4);//*/
		} catch (IOException e) {
			LOGGER.log(Level.INFO, "日志文件读取logUnit失败:" + DMSetting.logFileName);
		}
	}
	
	private LogUnit readLogUnit() {
		int transactionId = -1;
		TransactionType type = null;
		int virtualAddress = -1;
		int oldLen = -1;
		DataBlock oldItem = null;
		int newLen = -1;
		DataBlock newItem = null;
		try {
			transactionId = logFile.readInt();
			type = TransactionType.readFromFile(logFile);
			virtualAddress = logFile.readInt();
			oldLen = logFile.readInt();
			oldItem = DataBlock.readFromFile(logFile, oldLen);
			newLen = logFile.readInt();
			newItem = DataBlock.readFromFile(logFile, newLen);
		} catch (IOException e) {
			
		} 
		//logFile.readInt();//把Unit的大小读了,在这里没用,不需要了,这一行
		return new LogUnit(transactionId, type, virtualAddress, oldItem, newItem);
	}
	
	public void rollback() {
		try {
			logFile.seek(0);
			while (logFile.getFilePointer() != logFile.length()) {
				LogUnit unit = readLogUnit();
				if (unit.transactionId == TransactionManager.SUPER_ID || 
						unit.type == TransactionType.active) {
					redo(unit);
					itemRelease(unit);
				} else if (unit.type == TransactionType.start) {
					undoList.add(unit.transactionId);
				} else {//unit.type == TransactionType.commit || TransactionType.abort
					undoList.remove(unit.transactionId);
				}
			}
			
		} catch (IOException e) {
			LOGGER.log(Level.INFO, "日志文件重放失败:" + DMSetting.logFileName);
		}
		Revoke();
	}
	
	private void itemRelease(LogUnit unit) {
		unit.newItem.release();
		//因为全部redo,所以不需要oldItem,所以都是null
		//unit.oldItem.release();
	}
	
	private void redo(LogUnit unit) {
		int physicalAddress = mmu.changeVirtualAddressToPhysicalAddress(unit.virtualAddress);
		cache.update(physicalAddress, unit.newItem);
		mmu.modifyPage(unit.virtualAddress);
	}
	
	void Revoke() {
		for (Integer undoXid : undoList) {
			tm.abort(undoXid);
		}
	}
}
