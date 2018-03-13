package versionmanager;

class Request {
	
	int virtualAddress;
	byte[] dataItem;
	InvokeType type;
	
	Request(int virtualAddress, byte[] dataItem, InvokeType type) {
		this.virtualAddress = virtualAddress;
		this.dataItem = dataItem;
		this.type = type;
	}
	
}
