package indexmanager.current;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class IndexItem {
	
	private final ReentrantReadWriteLock rwlock = new ReentrantReadWriteLock();
	
	//当前项被加锁多少次
	private AtomicInteger count = new AtomicInteger(0);
	
	//用来保障锁升级,不一定能用到.....
	private final ReentrantLock lock = new ReentrantLock();
	
	final int address;
	
	IndexItem(int address) {
		this.address = address;
	}
	
	boolean canClear() {
		return count.compareAndSet(0, 0);
	}
	
	void lockX() {
		count.incrementAndGet();
		lock.lock();
		rwlock.writeLock().lock();
		lock.unlock();
	}
	
	void lockS() {
		count.incrementAndGet();
		lock.lock();
		rwlock.readLock().lock();
		lock.unlock();
	}
	
	void unlockS() {
		count.decrementAndGet();
		rwlock.readLock().unlock();
	}
	
	void unlockX() {
		count.decrementAndGet();
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
