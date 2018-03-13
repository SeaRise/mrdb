package transactionManager;

import java.io.IOException;
import java.io.RandomAccessFile;

import datamanager.DataManager;
import datamanager.OutOfDiskSpaceException;

public class TransactionManager {
	
	private DataManager dm = DataManager.getInstance();
	
	public static final int SUPER_ID = 0; //这个事务ID是redo_only事务的编号,即是一条记录就是一个事务
	
	public static final int MAX_TRANSACTION_ID = 5000;//transactionId的最大值,一旦达到这个值就到达了检查点.
	
	private int transactionId = -1;
	
	private RandomAccessFile tmFile = null;
	
	public TransactionManager() {
		try {
			tmFile = new RandomAccessFile(TMSetting.tmFileName, "rw");
			if (tmFile.length() == 0) {
				updateTransactionId(1);
				transactionId = 1;
			} else {
				transactionId = getTransactionId();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private int getTransactionId() throws IOException {
		tmFile.seek(0);
		return tmFile.readInt();
	}
	
	private void updateTransactionId(int transactionId) {
		try {
			tmFile.seek(0);
			tmFile.writeInt(transactionId);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	
	public void startTransaction() {
		//引入检查点,强制更新刷新入磁盘
		if (transactionId == MAX_TRANSACTION_ID) {
			dm.rollback();
			//把缓存刷入磁盘
			dm.flush();
			//将已被检查的日志删除,即是将日志文件清空
			dm.clearLogFile();
			//初始化transactionId
			initTransactionId();
		}
		dm.start(transactionId);
	}
	
	private void initTransactionId() {
		transactionId = 1;
		updateTransactionId(transactionId);
	}
	
	public void abortTransaction() {
		dm.abort(transactionId++);
		updateTransactionId(transactionId);
	}
	
	public void commitTransaction() {
		dm.commit(transactionId++);
		updateTransactionId(transactionId);
	}
	
	public byte[] read(int virtualAddress) {
		return dm.read(virtualAddress);
	}
	
	public int insert(byte[] dataItem, boolean isTransaction) throws OutOfDiskSpaceException {
		return isTransaction ? transactionInsert(dataItem) : onlyInsert(dataItem);
	}
	
	private int onlyInsert(byte[] dataItem) throws OutOfDiskSpaceException {
		return dm.insert(dataItem, SUPER_ID);
	}
	
	private int transactionInsert(byte[] dataItem) throws OutOfDiskSpaceException {
		return dm.insert(dataItem, transactionId);
	}
	
	public void update(int virtualAddress, byte[] dataItem, boolean isTransaction) {
		if (isTransaction) {
			transactionUpdate(virtualAddress, dataItem);
		} else {
			onlyUpdate(virtualAddress, dataItem);
		}
	}
	
	private void onlyUpdate(int virtualAddress, byte[] dataItem) {
		dm.update(virtualAddress, dataItem, SUPER_ID);
	}
	
	private void transactionUpdate(int virtualAddress, byte[] dataItem) {
		dm.update(virtualAddress, dataItem, transactionId);
	}
}
