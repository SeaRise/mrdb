package versionmanager;

import java.util.concurrent.locks.ReentrantReadWriteLock;

class Version {
	final int address;
	private final ReentrantReadWriteLock rwlock = new ReentrantReadWriteLock();
	
	Version(int address) {
		this.address = address;
	}
	
	void lockX() {
		rwlock.writeLock().lock();
	}
	
	boolean tryLockX() {
		return rwlock.writeLock().tryLock();
	}
	
	void unlockX() {
		rwlock.writeLock().unlock();
	}
	
	void lockS() {
		rwlock.readLock().lock();
	}
	
	boolean tryLockS() {
		return rwlock.readLock().tryLock();
	}
	
	void unlockS() {
		rwlock.readLock().unlock();
	}
}
