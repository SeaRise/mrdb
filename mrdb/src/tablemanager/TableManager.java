package tablemanager;

import util.ParentPath;
import util.pool.DataBlock;


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
	
	public void createTable(String tableName, Type keyType) {
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
	
	public void selectTable(String tableName) {
		exec.selectTable(tableName);
	}
	
	public void insert(Object key, DataBlock value, boolean isTransaction) {
		exec.insert(key, value, isTransaction);
	}
	
	public void update(Object key, DataBlock newValue, boolean isTransaction) {
		exec.update(key, newValue, isTransaction);
	}
	
	public DataBlock read(Object key, boolean isTransaction) {
		return exec.read(key, isTransaction);
	}
	
	public void delete(Object key, boolean isTransaction) {
		exec.delete(key, isTransaction);
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
