package transactionManager;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.logging.Level;
import java.util.logging.Logger;

import util.ParentPath;

public class TransactionManager {
	
	// Logger
	private final static Logger LOGGER = Logger.getLogger(TransactionManager.class.getName());
	
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
	
	private RandomAccessFile getAccessFile() {
		if (tmFile.get() == null) {
			try {
				tmFile.set(new RandomAccessFile(tmFileName, "rw"));
			} catch (FileNotFoundException e) {
				LOGGER.log(Level.INFO, "事务管理文件读取失败:" + tmFileName);
			}
		}
		return tmFile.get();
	}
	
	//只有start+锁是因为其他的方法不会地址重复,只有start会
	public synchronized int start() {
		RandomAccessFile tmFile = getAccessFile();
		int xid = -1;
		try {
			xid = ((int)tmFile.length())+1;
			transactionId.set(xid);
			tmFile.seek(xid-1);
			tmFile.write(XID.active.getByte());
		} catch (IOException e) {
			LOGGER.log(Level.INFO, "start一个新事务失败,xid = " + xid);
		}
		
		LOGGER.log(Level.INFO, "start开始一个事务,xid = " + xid);
		
		return xid;
	}
	
	public void commit() {		
		RandomAccessFile tmFile = getAccessFile();
		int xid = -1;
		try {
			tmFile.seek((xid = getXID())-1);
			tmFile.write(XID.commit.getByte());
		} catch (IOException e) {
			LOGGER.log(Level.INFO, "commit一个事务失败,xid = " + xid);
		}
		LOGGER.log(Level.INFO, "commit提交一个事务,xid = " + getXID());
	}
	
	public void abort() {
		RandomAccessFile tmFile = getAccessFile();
		int xid = -1;
		try {
			tmFile.seek((xid = getXID())-1);
			tmFile.write(XID.aborted.getByte());
		} catch (IOException e) {
			LOGGER.log(Level.INFO, "abort一个事务失败,xid = " + xid);
		}
		LOGGER.log(Level.INFO, "abort回滚一个事务,xid = " + xid);
	}
	
	public void abort(int xid) {
		RandomAccessFile tmFile = getAccessFile();
		try {
			tmFile.seek(xid-1);
			tmFile.write(XID.aborted.getByte());
		} catch (IOException e) {
			LOGGER.log(Level.INFO, "abort一个事务失败,xid = " + xid);
		}
		LOGGER.log(Level.INFO, "abort回滚一个事务,xid = " + xid);
	}
	
	public XID getXidState(int xid) {
		RandomAccessFile tmFile;
		XID x = null;
		try {
			tmFile = getAccessFile();
			tmFile.seek(xid-1);
			x = XID.getXID(tmFile.readByte());
		} catch (IOException e) {
			LOGGER.log(Level.INFO, "读取事务状态出错,xid = " + xid);
		}
		
		return x;
	}
	
	public int getXID() {
		return transactionId.get();
	}
}
