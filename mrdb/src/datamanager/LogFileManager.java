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
			//计算出Unit的大小,便于倒着读文件
			logFile.writeInt(4+2+4+ // transactionId+type+virtualAddress
					4+(oldItem == null ? 0 : oldItem.length)+ // oldItem.length+oldItem
					4+(newItem == null ? 0 : newItem.length)+ // newItem.length+newItem
					4);//Unit大小这个量的大小
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
		logFile.readInt();//把Unit的大小读了,在这里没用
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
				undoTransactionId(unit.transactionId);
			} else {//unit.type == TransactionType.commit || TransactionType.abort
				undoList.remove(unit.transactionId);
			}
		}
		Revoke();
	}
	
	private void redo(LogUnit unit) {
		int physicalAddress = mmu.changeVirtualAddressToPhysicalAddress(unit.virtualAddress);
		cache.update(physicalAddress, unit.newItem);
		mmu.modifyPage(unit.virtualAddress);
	}
	
	void undoTransactionId(int transactionId) {
		undoList.add(transactionId);
	}
	
	void Revoke() throws IOException {
		long offset = logFile.length();
		long lenOffset;
		while (offset != 0) {
			lenOffset = offset-4;
			logFile.seek(lenOffset);
			offset -= logFile.readInt();
			logFile.seek(offset);
			undo(readLogUnit());
		}
		undoList.clear();
	}
	
	private void undo(LogUnit unit) throws IOException {
		if (undoList.contains(unit.transactionId)) {
			if (unit.type == TransactionType.start) {
				undoList.remove(unit.transactionId);
				if (undoList.isEmpty()) {
					return;
				}
			} else if (unit.type == TransactionType.active) {
				int physicalAddress = 
						mmu.changeVirtualAddressToPhysicalAddress(unit.virtualAddress);
				cache.update(physicalAddress, unit.oldItem);
				mmu.modifyPage(unit.virtualAddress);
			}
		}
	}

	public void clear() {
		try {
			logFile.setLength(0);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
