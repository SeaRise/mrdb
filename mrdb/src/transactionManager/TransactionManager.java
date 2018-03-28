package transactionManager;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import datamanager.DataManager;

import util.ParentPath;

public class TransactionManager {
	public static final int SUPER_ID = 0; //这个事务ID是redo_only事务的编号,即是一条记录就是一个事务
	
	static String tmFileName = ParentPath.tmFileParentName+"tmFile";
	
	private ThreadLocal<Integer> transactionId = new ThreadLocal<Integer>();
	
	private static TransactionManager tm = new TransactionManager();
	
	private TransactionManager() {
	}
	
	static public TransactionManager getInstance() {
		return tm;
	}
	
	private RandomAccessFile getAccessFile() throws FileNotFoundException {
		return new RandomAccessFile(tmFileName, "rw");
	}
	
	public synchronized int start() throws IOException {
		RandomAccessFile tmFile = getAccessFile();
		int xid = (int) tmFile.length()+1;
		transactionId.set(xid);
		tmFile.seek(xid-1);
		tmFile.write(XID.active.getByte());
		tmFile.close();
		return xid;
	}
	
	public void commit() throws IOException {
		RandomAccessFile tmFile = getAccessFile();
		tmFile.seek(getXID()-1);
		tmFile.write(XID.active.getByte());
	}
	
	public synchronized void abort() throws IOException {
		RandomAccessFile tmFile = getAccessFile();
		tmFile.seek(getXID()-1);
		tmFile.write(XID.aborted.getByte());
	}
	
	public XID getXidState(int xid) throws IOException {
		RandomAccessFile tmFile = getAccessFile();
		tmFile.seek(xid-1);
		return XID.getXID(tmFile.readByte());
	}
	
	public int getXID() {
		return transactionId.get();
	}
}
