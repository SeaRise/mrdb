package versionmanager;

class TranRequest {
	
	final int transactionId;
	
	private Lock lock;
	
	TranRequest(int transactionId, Lock lock) {
		this.transactionId = transactionId;
		this.lock = lock;
	}
	
	Lock getLock() {
		return lock;
	}
	
	void upgrate() {
		lock = Lock.lockX;
	}
}
