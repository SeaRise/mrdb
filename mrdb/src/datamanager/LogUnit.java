package datamanager;

import transactionManager.TransactionType;

class LogUnit {
	
	int transactionId; 
	TransactionType type; 
	int virtualAddress; 
	byte[] oldItem; 
	byte[] newItem;
	
	LogUnit(int transactionId, TransactionType type, int virtualAddress, byte[] oldItem, byte[] newItem) {
		this.transactionId = transactionId;
		this.type = type;
		this.virtualAddress = virtualAddress;
		this.oldItem = oldItem;
		this.newItem = newItem;
	}
	
}
