package com.mrdb.im.current;

import java.util.concurrent.locks.ReentrantReadWriteLock;

class IndexItem {
	
	private final ReentrantReadWriteLock rwlock = new ReentrantReadWriteLock();
	
	//当前项被加锁多少次
	//private AtomicInteger count = new AtomicInteger(0);
	
	final int address;
	
	IndexItem(int address) {
		this.address = address;
	}
	
	/*
	boolean canClear() {
		return count.compareAndSet(0, 0);
	}*/
	
	void lockX() {
		//count.incrementAndGet();
		rwlock.writeLock().lock();
	}
	
	void lockS() {
		//count.incrementAndGet();
		rwlock.readLock().lock();
	}
	
	void unlockS() {
		//count.decrementAndGet();
		rwlock.readLock().unlock();
	}
	
	void unlockX() {
		//count.decrementAndGet();
		rwlock.writeLock().unlock();
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
