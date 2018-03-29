package datamanager;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayDeque;

import transactionManager.TransactionManager;
import transactionManager.TransactionType;
import util.pool.DataBlock;

class LogFileManager {
	
	private RandomAccessFile logFile = null;
	
	private DBcache cache = DBcache.getInstance();
	
	private MMU mmu = MMU.getInstance();
	
	static private LogFileManager lfm = new LogFileManager();
	
	private static TransactionManager tm = TransactionManager.getInstance();
	
	private ArrayDeque<Integer> undoList = new ArrayDeque<Integer>();
	
	static LogFileManager getInstance() {
		return lfm;
	}
	
	private LogFileManager() {
		try {
			logFile = new RandomAccessFile(DMSetting.logFileName, "rw");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	void login(int transactionId, TransactionType type, int virtualAddress, 
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
			e.printStackTrace();
		}
	}
	
	private LogUnit readLogUnit() throws IOException {
		int transactionId = logFile.readInt(); 
		TransactionType type = TransactionType.readFromFile(logFile); 
		int virtualAddress = logFile.readInt(); 
		int oldLen = logFile.readInt();
		DataBlock oldItem =  DataBlock.readFromFile(logFile, oldLen);
		int newLen = logFile.readInt();		
		DataBlock newItem =  DataBlock.readFromFile(logFile, newLen);
		//logFile.readInt();//把Unit的大小读了,在这里没用,不需要了,这一行
		return new LogUnit(transactionId, type, virtualAddress, oldItem, newItem);
	}
	
	void rollback() throws IOException {
		logFile.seek(0);
		while (logFile.getFilePointer() != logFile.length()) {
			LogUnit unit = readLogUnit();
			if (unit.transactionId == TransactionManager.SUPER_ID || 
					unit.type == TransactionType.active) {
				redo(unit);
			} else if (unit.type == TransactionType.start) {
				undoList.add(unit.transactionId);
			} else {//unit.type == TransactionType.commit || TransactionType.abort
				undoList.remove(unit.transactionId);
			}
			itemRelease(unit);
		}
		Revoke();
	}
	
	private void itemRelease(LogUnit unit) {
		unit.newItem.release();
		unit.oldItem.release();
	}
	
	private void redo(LogUnit unit) {
		int physicalAddress = mmu.changeVirtualAddressToPhysicalAddress(unit.virtualAddress);
		cache.update(physicalAddress, unit.newItem);
		mmu.modifyPage(unit.virtualAddress);
	}
	
	void Revoke() throws IOException {
		for (Integer undoXid : undoList) {
			tm.abort(undoXid);
		}
	}
}
