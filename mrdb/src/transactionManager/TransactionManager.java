package transactionManager;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import util.ParentPath;

public class TransactionManager {
	public static final int SUPER_ID = 0; //这个事务ID是redo_only事务的编号,即是一条记录就是一个事务
	
	static String tmFileName = ParentPath.tmFileParentName+"tmFile";
	
	private ThreadLocal<Integer> transactionId = new ThreadLocal<Integer>();
	
	private ThreadLocal<RandomAccessFile> tmFile = new ThreadLocal<RandomAccessFile>();
	
	private static TransactionManager tm = new TransactionManager(); 
	
	private TransactionManager() {
	}
	
	static public TransactionManager getInstance() {
		return tm;
	}
	
	private RandomAccessFile getAccessFile() throws FileNotFoundException {
		//return new RandomAccessFile(tmFileName, "rw");
		if (tmFile.get() == null) {
			tmFile.set(new RandomAccessFile(tmFileName, "rw"));
		}
		return tmFile.get();
	}
	
	//只有start+锁是因为其他的方法不会地址重复,只有start会
	public synchronized int start() throws IOException {
		RandomAccessFile tmFile = getAccessFile();
		int xid = ((int)tmFile.length())+1;
		transactionId.set(xid);
		tmFile.seek(xid-1);
		tmFile.write(XID.active.getByte());
		return xid;
	}
	
	public void commit() throws IOException {
		RandomAccessFile tmFile = getAccessFile();
		tmFile.seek(getXID()-1);
		tmFile.write(XID.active.getByte());
	}
	
	public void abort() throws IOException {
		RandomAccessFile tmFile = getAccessFile();
		tmFile.seek(getXID()-1);
		tmFile.write(XID.aborted.getByte());
	}
	
	public void abort(int xid) throws IOException {
		RandomAccessFile tmFile = getAccessFile();
		tmFile.seek(xid-1);
		tmFile.write(XID.aborted.getByte());
	}
	
	public XID getXidState(int xid) throws IOException {
		RandomAccessFile tmFile = getAccessFile();
		tmFile.seek(xid-1);
		XID x = XID.getXID(tmFile.readByte());
		return x;
	}
	
	public int getXID() {
		return transactionId.get();
	}
}
