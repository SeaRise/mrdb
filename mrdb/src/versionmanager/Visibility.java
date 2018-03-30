package versionmanager;

import java.io.IOException;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import transactionManager.TransactionManager;
import transactionManager.XID;
import util.Entry;


class Visibility {
	
	private static ThreadLocal<Integer> level = VersionManager.getInstance().level;
	
	private static ThreadLocal<Set<Integer>> activeTran = VersionManager.getInstance().activeSnapShot;
	
	private static TransactionManager tm = TransactionManager.getInstance();
	
	// Logger
	private final static Logger LOGGER = Logger.getLogger(Visibility.class.getName());
	
	static boolean IsVisible(int xid, Entry e) throws IOException {
		Integer lev = level.get();
		if (lev == null || lev.equals(0)) {
			
			return readCommitted(xid, e);
		} else {
			return repeatableRead(xid, e);
		}
	}
	
	//读已提交
	static boolean readCommitted(int xid, Entry e) throws IOException {
		int xmin = e.xmin;
		int xmax = e.xmax;
		
		//该版本由自己创建且未删除。
		if (xmin == xid && xmax == 0) {
			//LOGGER.log(Level.INFO, "读已提交,可见性判断1true,xid = " + xid + " xmin = " + xmin + " xmax = " + xmax);
			return true;
		}
		
		boolean isCommitted = tm.getXidState(xmin).equals(XID.commit);
		if (isCommitted) {
			//该版本由已commit的事务产生,且未被删除.
			if (xmax == 0) {
				//LOGGER.log(Level.INFO, "读已提交,可见性判断2true,xid = " + xid + " xmin = " + xmin + " xmax = " + xmax);
				return true;
			}
			if (xmax != xid) {
				isCommitted = tm.getXidState(xmax).equals(XID.commit);
				//该版本由已commit的事务产生,且删除的事务未提交或不是自己删除的.
				if (!isCommitted) {
					//LOGGER.log(Level.INFO, "读已提交,可见性判断3true,xid = " + xid + " xmin = " + xmin + " xmax = " + xmax);
					return true;
				}
			}
		}
		LOGGER.log(Level.INFO, "读已提交,可见性判断false,xid = " + xid + " xmin = " + xmin + " xmax = " + xmax);
		return false;
	}
	
	//可重复读
	static boolean repeatableRead(int xid, Entry e) throws IOException {
		int xmin = e.xmin;
		int xmax = e.xmax;
		
		LOGGER.log(Level.INFO, "可重复读,可见性判断,xid = " + xid + " xmin = " + xmin + " xmax = " + xmax);
		
		//该版本由自己创建且未删除。
		if (xmin == xid && xmax == 0) {
			return true;
		}
		
		boolean isCommitted = tm.getXidState(xmin).equals(XID.commit);
		//该版本由比xid小的已提交的事务产生
		if (isCommitted && xmin < xid) {
			//未被删除.
			if (xmax == 0) {
				return true;
			}
			//删除该版本的事务,不是自己
			if (xmax != xid) {
				isCommitted = tm.getXidState(xmax).equals(XID.commit);
				//删除的事务未提交,或这大于xid,或者xid开始时还未提交.
				if (isCommitted == false || xmax > xid || InSnapShot(xmax)) {
					return true;
				}
			}
		}
		return false;
	}
	
	static boolean InSnapShot(int xid) {
		return activeTran.get().contains(xid);
	}
}
