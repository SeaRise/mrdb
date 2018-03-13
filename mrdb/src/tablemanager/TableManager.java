package tablemanager;

import util.ParentPath;
import datamanager.OutOfDiskSpaceException;

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
	/*
	public void createTable(String tableName, String[] fieldNames, 
			Type[] types, boolean[] isEstablishIndex) {
		try {
			exec.createTable(tableName, fieldNames, types, isEstablishIndex);
		} catch (TableNameRepeatException e) {
			System.out.println("table name has exist");
		}
	}
	
	/*
	public void deleteTable(String tableName) {
		try {
			exec.deleteTable(tableName);
		} catch (TableNotFoundException e) {
			System.out.println("table not found");
		}
	}*/
	/*
	public void insert(String tableName, DataType[] dataTypes, boolean isTransaction) {
		try {
			exec.insert(tableName, dataTypes, isTransaction);
		} catch (TableNotFoundException e) {
			System.out.println("table not found");
		} catch (OutOfDiskSpaceException e) {
			System.out.println("disk has no more spaces");
		} 
	}
	
	public void update(String tableName, DataType[] keyTypes, int[] keyIndexes, 
			Object[] newValues, int[] valueIndexes, boolean isTransaction) {
		try {
			System.out.println(exec.update(
					tableName, keyTypes, keyIndexes, newValues, valueIndexes, isTransaction));
		} catch (TableNotFoundException e) {
			System.out.println("table not found");
		} 
	}
	
	public void read(String tableName, DataType[] keyTypes, int[] keyIndexes) {
		try {
			DataType[][] dt = exec.read(tableName, keyTypes, keyIndexes);
			if (dt == null) {
				System.out.println("not found");
			} else {
				boolean hasRecord = false;
				for (int i = 0; i < dt.length; i++) {
					if (dt[i] == null) {
						continue;
					}
					hasRecord = true;
					for (int j = 0; j < dt[i].length; j++) {
						System.out.print(dt[i][j] + " ");
					}
					System.out.println();
				}
				if (!hasRecord) {
					System.out.println("not found");
				}
			}
		} catch (TableNotFoundException e) {
			System.out.println("table not found");
		} 
	}*/
	
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
