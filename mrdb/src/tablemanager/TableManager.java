package tablemanager;

import indexmanager.IndexDuplicateException;

import java.io.FileNotFoundException;
import java.io.IOException;

import util.ParentPath;
import datamanager.OutOfDiskSpaceException;
import datamanager.pool.DataBlock;

/*一个疑惑:
 * 多线程事务是指一个用户连接内的多个事务并行还是多个用户连接的事务并行
 * 目前以后者为准
 * */
public class TableManager {
	
	private TBMExecutor exec = null;
	
	public TableManager() {
		ParentPath.createPath();
		exec = new TBMExecutor();
	}
	
	public void createTable(String tableName, Type keyType) throws TableNameRepeatException, IOException, OutOfDiskSpaceException {
		exec.createTable(tableName, keyType);
	}
	
	public void deleteTable(String tableName) {
		exec.deleteTable(tableName);
	}
	
	public void selectTable(String tableName) throws IOException, TableNotFoundException {
		exec.selectTable(tableName);
	}
	
	public void insert(Object key, DataBlock value, boolean isTransaction) throws NotSelectTableException, ObjectMismatchException, OutOfDiskSpaceException, IndexDuplicateException {
		exec.insert(key, value, isTransaction);
	}
	
	public void update(Object key, DataBlock newValue, boolean isTransaction) throws NotSelectTableException, ObjectMismatchException {
		exec.update(key, newValue, isTransaction);
	}
	
	public DataBlock read(Object key) throws NotSelectTableException, ObjectMismatchException {
		return exec.read(key);
	}
	
	public void start() {
		exec.start();
	}
	
	public void commit() {
		exec.commit();
	}
	
	public void abort() {
		exec.abort();
	}
}
