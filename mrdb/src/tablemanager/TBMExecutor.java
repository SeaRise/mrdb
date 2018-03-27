package tablemanager;

import indexmanager.IndexDuplicateException;
import indexmanager.IndexManager;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import datamanager.OutOfDiskSpaceException;
import datamanager.pool.DataBlock;

import transactionManager.TransactionManager;
import util.DataUtil;
import util.ParentPath;

public class TBMExecutor {
	
	private static String TMB_END = "end";
	
	private IndexManager im = new IndexManager();
	private TransactionManager tm = new TransactionManager();
	
	private String tableName = null;
	private Type keyType = null;
	private int keyAddress = -1;
	
	TBMExecutor() {
	}
	
	void createTable(String tableName, Type keyType) throws TableNameRepeatException, TableCreateFailExceotion {
		File file = new File(ParentPath.tablesFileParentName + tableName + ".t");
		try {
			doCreateTable(file, keyType);
		} catch (IOException | OutOfDiskSpaceException e) {
			file.delete();
			throw new TableCreateFailExceotion();
		}
	}
	
	private void doCreateTable(File file, Type keyType) throws IOException, TableNameRepeatException, OutOfDiskSpaceException {	
		if (file.exists()) {
			DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
			try {
				dis.readUTF();
				dis.readUTF();
				dis.readInt();
				boolean flag = dis.readUTF().equals(TMB_END);
				dis.close();
				if (flag) {
					throw new TableNameRepeatException();
				}
				file.delete();
			} finally {
				dis.close();
			}
			
		}
		file.createNewFile();
		DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
		try {
			dos.writeUTF(tableName);
			dos.writeUTF(keyType.toString());
			dos.writeInt(initKey(keyType));
			dos.writeUTF(TMB_END);
		} finally {
			dos.close();
		}
	}
	
	private int initKey(Type keyType) throws OutOfDiskSpaceException {
		return im.addRootNode(keyType);
	}
	
	
	void deleteTable(String tableName) {
		File file = new File(ParentPath.tablesFileParentName + tableName + ".t");
		if (file.exists()) {
			file.delete();
		}
	}
	
	public void selectTable(String tableName) throws TableNotFoundException {
		File file = new File(ParentPath.tablesFileParentName + tableName + ".t");
		try {
			doSelectTable(file);
		} catch (IOException e) {
			this.tableName = null;
			this.keyType = null;
			this.keyAddress = -1;
			file.delete();
			throw new TableNotFoundException();
		}
	}
	
	private void doSelectTable(File file) throws TableNotFoundException, IOException {
		if (file.exists()) {
			DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
			try {
				this.tableName = dis.readUTF();
				this.keyType = Type.getType(dis.readUTF());
				this.keyAddress = dis.readInt();
				boolean flag = dis.readUTF().equals(TMB_END);
				
				if (flag) {
					this.tableName = null;
					this.keyType = null;
					this.keyAddress = -1;
					return;
				}
			} finally {
				dis.close();
			}
		}
		file.delete();
		throw new TableNotFoundException();
	}
	
	void start() {
		tm.startTransaction();
	}
	
	void commit() {
		tm.commitTransaction();
	}
	
	void abort() {
		tm.abortTransaction();
	}
	
    void insert(Object key, DataBlock value, boolean isTransaction) throws NotSelectTableException, ObjectMismatchException, OutOfDiskSpaceException, IndexDuplicateException {
    	check(key);
    	int valueAddress = tm.insert(value, isTransaction);
    	im.insert(key, valueAddress, keyAddress, keyType);
	}
	
	void update(Object key, DataBlock newValue, boolean isTransaction) throws NotSelectTableException, ObjectMismatchException {
		check(key);
		int address = im.search(key, keyType, keyAddress);
		tm.update(address, newValue, isTransaction);
	}
	
	DataBlock read(Object key) throws NotSelectTableException, ObjectMismatchException {
		check(key);
		int address = im.search(key, keyType, keyAddress);
		return tm.read(address);
	}
	
	private void check(Object key) throws NotSelectTableException, ObjectMismatchException {
		if (tableName == null) {
			throw new NotSelectTableException();
		}
		if (keyType == Type.int32) {
			if (!(key instanceof Integer)) {
				throw new ObjectMismatchException();
			}
		} else if (keyType == Type.long64) {
			if (!(key instanceof Long)) {
				throw new ObjectMismatchException();
			}
		} else if (keyType == Type.string64) {
			if (!(key instanceof String)) {
				throw new ObjectMismatchException();
			}
		} else {
			throw new ObjectMismatchException();
		}
	}
}
