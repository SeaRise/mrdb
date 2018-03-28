package datamanager;

import transactionManager.TransactionType;
import util.pool.DataBlock;

class LogUnit {
	
	int transactionId; 
	TransactionType type; 
	int virtualAddress; 
	DataBlock oldItem; 
	DataBlock newItem;
	
	LogUnit(int transactionId, TransactionType type, int virtualAddress, DataBlock oldItem, DataBlock newItem) {
		this.transactionId = transactionId;
		this.type = type;
		this.virtualAddress = virtualAddress;
		this.oldItem = oldItem;
		this.newItem = newItem;
	}
	
}
