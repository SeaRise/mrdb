package indexmanager.current;

import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class IndexItem {
	
	final ReentrantReadWriteLock rwlock = new ReentrantReadWriteLock();
	
	//用来保障锁升级
	final ReentrantLock lock = new ReentrantLock();
	
	final int address;
	
	IndexItem(int address) {
		this.address = address;
	}
	
	
	
	void lockX() {
		lock.lock();
		rwlock.writeLock().lock();
		lock.unlock();
	}
	
	void lockS() {
		lock.lock();
		rwlock.readLock().lock();
		lock.unlock();
	}
	
	void unlockS() {
		rwlock.readLock().unlock();
	}
	
	void unlockX() {
		rwlock.writeLock().unlock();
	}
	
	void update() {
		lock.lock();
		rwlock.readLock().unlock();
		rwlock.writeLock().lock();
		lock.unlock();
	}
	
	void degrade() {
		lock.lock();
		rwlock.writeLock().unlock();
		rwlock.readLock().lock();
		lock.unlock();
	}
	
	boolean isWriting() {
		return rwlock.writeLock().isHeldByCurrentThread();
	}

	@Override
	public int hashCode() {
		return address;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		
		if (obj == this) {
			return true;
		}
		
		if (!(obj instanceof IndexItem)) {
			return false;
		}
		
		IndexItem it = (IndexItem)obj;
		
		return address == it.address;
	}
	
}
