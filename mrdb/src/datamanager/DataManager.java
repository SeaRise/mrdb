package datamanager;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;

import transactionManager.TransactionType;
import util.pool.DataBlock;

/**执行顺序是:
 *    申请块
 *    记录日志
 *    更新cache
 * */
public class DataManager {
	
	transient final ReentrantLock lock = new ReentrantLock();
	
	private DBcache cache = DBcache.getInstance();
	
	private MMU mmu = MMU.getInstance();
	
	private PageManager pm = PageManager.getInstance();
	
	private LogFileManager lfm = LogFileManager.getInstance();
	
	private static DataManager dm = new DataManager();
	
	static public DataManager getInstance() {
		dm.pm.init(dm);
		return dm;
	}
	
	private DataManager() {
		rollback();
	}
	
	public void rollback() {
		final ReentrantLock lock = this.lock;
        lock.lock();
		try {
			lfm.rollback();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			lock.unlock();
		}
	}
	
	public void flush() {
		final ReentrantLock lock = this.lock;
        lock.lock();
		try {
			mmu.flush();
		} finally {
			lock.unlock();
		}
		
		
	}
	
	//物理地址只在DBCache中使用,对外提供虚拟地址.
	public int insert(DataBlock dataItem, int transactionId) throws OutOfDiskSpaceException {
		int virtualAddress = -1;
		int physicalAddress = -1;
		final ReentrantLock lock = this.lock;
        lock.lock();
		try {
			virtualAddress = pm.applyBlock(dataItem.length + 4);//4为dataItem长度的字节数
		} finally {
			lock.unlock();
		}
		
		lock.lock();
		try {
			lfm.login(transactionId, TransactionType.active, virtualAddress, null, dataItem);
		} finally {
			lock.unlock();
		}
		
		lock.lock();
		try {
			physicalAddress = mmu.changeVirtualAddressToPhysicalAddress(virtualAddress);
			cache.update(physicalAddress, dataItem);
			mmu.modifyPage(virtualAddress);
		} finally {
			lock.unlock();
		}
		
		return virtualAddress;
	}
	
	public DataBlock read(int virtualAddress) {
		int physicalAddress = -1;
		DataBlock dataItem = null;
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			physicalAddress = mmu.changeVirtualAddressToPhysicalAddress(virtualAddress);
			dataItem = cache.read(physicalAddress);
		} finally {
			lock.unlock();
		}
		return dataItem;
	}
	
	public void update(int virtualAddress, DataBlock dataItem, int transactionId) {
		int physicalAddress = -1;
		DataBlock oldItem = null;
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			physicalAddress = mmu.changeVirtualAddressToPhysicalAddress(virtualAddress);
			oldItem = cache.read(physicalAddress);
		} finally {
			lock.unlock();
		}
		
		lock.lock();
		try {
			lfm.login(transactionId, TransactionType.active, virtualAddress, oldItem, dataItem);
		} finally {
			lock.unlock();
		}
		
		lock.lock();
		try {
			cache.update(physicalAddress, dataItem);
			mmu.modifyPage(virtualAddress);
		} finally {
			lock.unlock();
		}
	}
	
	public void start(int transactionId) {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			lfm.login(transactionId, TransactionType.start, -1, null, null);
		} finally {
			lock.unlock();
		}
	}
	
	public void commit(int transactionId) {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			lfm.login(transactionId, TransactionType.commit, -1, null, null);
		} finally {
			lock.unlock();
		}
	}

	public void abort(int transactionId) {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			lfm.undoTransactionId(transactionId);
		} finally {
			lock.unlock();
		}
		
		lock.lock();
		try {
			lfm.Revoke();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			lock.unlock();
		}
	}

	public void clearLogFile() {
		final ReentrantLock lock = this.lock;
		lock.lock();
		try {
			lfm.clear();
		} finally {
			lock.unlock();
		}
	}
}
