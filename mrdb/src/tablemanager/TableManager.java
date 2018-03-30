package tablemanager;

import indexmanager.IndexDuplicateException;

import java.io.IOException;
import java.util.logging.Logger;

import transactionManager.TransactionManager;
import util.ParentPath;
import util.pool.DataBlock;
import datamanager.OutOfDiskSpaceException;


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
	
	void upgradeLevel() {
		exec.upgradeLevel();
	}
	
	void degradeLevel() {
		exec.degradeLevel();
	}
	
	public void selectTable(String tableName) throws IOException, TableNotFoundException {
		exec.selectTable(tableName);
	}
	
	public void insert(Object key, DataBlock value, boolean isTransaction) throws NotSelectTableException, ObjectMismatchException, OutOfDiskSpaceException, IndexDuplicateException, IOException {
		exec.insert(key, value, isTransaction);
	}
	
	public void update(Object key, DataBlock newValue, boolean isTransaction) throws NotSelectTableException, ObjectMismatchException, IOException, OutOfDiskSpaceException {
		exec.update(key, newValue, isTransaction);
	}
	
	public DataBlock read(Object key, boolean isTransaction) throws NotSelectTableException, ObjectMismatchException, IOException {
		return exec.read(key, isTransaction);
	}
	
	public void delete(Object key, boolean isTransaction) throws IOException, NotSelectTableException, ObjectMismatchException {
		exec.delete(key, isTransaction);
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
