package tablemanager;

import indexmanager.IndexDuplicateException;

import java.io.IOException;

import util.ParentPath;
import datamanager.OutOfDiskSpaceException;
import datamanager.pool.DataBlock;


public class TableManager {
	
	private TBMExecutor exec = null;
	
	private static TableManager tbm = new TableManager();
	
	static public TableManager getInstance() {
		return tbm;
	}
	
	private TableManager() {
		ParentPath.createPath();
		exec = new TBMExecutor();
	}
	
	public void createTable(String tableName, Type keyType) throws TableNameRepeatException, IOException, OutOfDiskSpaceException, TableCreateFailExceotion {
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
	
	public void start() throws IOException {
		exec.start();
	}
	
	public void commit() throws IOException {
		exec.commit();
	}
	
	public void abort() throws IOException {
		exec.abort();
	}
}
