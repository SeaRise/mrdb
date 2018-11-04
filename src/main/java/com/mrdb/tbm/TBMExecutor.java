package com.mrdb.tbm;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.mrdb.im.IndexManager;
import com.mrdb.tbm.exception.NotSelectTableException;
import com.mrdb.tbm.exception.ObjectMismatchException;
import com.mrdb.tbm.exception.TableCreateFailExceotion;
import com.mrdb.tbm.exception.TableNameRepeatException;
import com.mrdb.tbm.exception.TableNotFoundException;
import com.mrdb.util.ParentPath;
import com.mrdb.util.pool.DataBlock;
import com.mrdb.vm.VersionManager;

public class TBMExecutor {
	
	private static String TMB_END = "end";
	
	private IndexManager im = new IndexManager();
	private VersionManager vm = VersionManager.getInstance();
	
	private ThreadLocal<String> tableName = new ThreadLocal<String>();
	private ThreadLocal<Type> keyType = new ThreadLocal<Type>();
	private ThreadLocal<Integer> keyAddress = new ThreadLocal<Integer>();
	
	// Logger
	private final static Logger LOGGER = Logger.getLogger(TBMExecutor.class.getName());	
	
	TBMExecutor() {
	}
	
	synchronized void createTable(String tableName, Type keyType) {
		File file = new File(ParentPath.tablesFileParentName + tableName + ".t");
		try {
			doCreateTable(tableName, file, keyType);
		} catch (IOException e) {
			file.delete();
			try {
				throw new TableCreateFailExceotion();
			} catch (TableCreateFailExceotion e1) {
				LOGGER.log(Level.INFO, "create table fail, table name:" + tableName);
			}
		}
	}
	
	private void doCreateTable(String tableName, File file, Type keyType) throws IOException {	
		if (file.exists()) {
			DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
			try {
				dis.readUTF();
				dis.readUTF();
				dis.readInt();
				boolean flag = dis.readUTF().equals(TMB_END);
				dis.close();
				if (flag) {
					try {
						throw new TableNameRepeatException();
					} catch (TableNameRepeatException e) {
						LOGGER.log(Level.INFO, "table name has repeat, table name:" + tableName);
					}
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
	
	private int initKey(Type keyType) {
		return im.addRootNode(keyType);
	}
	
	
	synchronized void deleteTable(String tableName) {
		File file = new File(ParentPath.tablesFileParentName + tableName + ".t");
		if (file.exists()) {
			file.delete();
		}
	}
	
	void upgradeLevel() {
		vm.upgradeLevel();
	}
	
	void degradeLevel() {
		vm.degradeLevel();
	}
	
	synchronized void selectTable(String tableName) {
		File file = new File(ParentPath.tablesFileParentName + tableName + ".t");
		try {
			doSelectTable(file);
		} catch (IOException e) {
			this.tableName.set(null);
			this.keyType.set(null);
			this.keyAddress.set(null);
			file.delete();
			try {
				throw new TableNotFoundException();
			} catch (TableNotFoundException e1) {
				LOGGER.log(Level.INFO, "select table fail, tableName:" + tableName);
			}
		} catch (TableNotFoundException e) {
			LOGGER.log(Level.INFO, "select table fail, tableName:" + tableName);
		}
	}
	
	private void doSelectTable(File file) throws TableNotFoundException, IOException {
		if (file.exists()) {
			DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
			try {
				this.tableName.set(dis.readUTF());
				this.keyType.set(Type.getType(dis.readUTF()));
				this.keyAddress.set(dis.readInt());
				boolean flag = dis.readUTF().equals(TMB_END);
				
				if (flag) {
					return;
				}
			} finally {
				dis.close();
			}
		}
		this.tableName.set(null);
		this.keyType.set(null);
		this.keyAddress.set(null);
		file.delete();
		throw new TableNotFoundException();
	}
	
	void start() {
		vm.startTransaction();
	}
	
	void commit() {
		vm.commitTransaction();
	}
	
	void abort() {
		vm.abortTransaction();
	}
	
    void insert(Object key, DataBlock value, boolean isTransaction) {
    	check(key);
    	if (!isTransaction) {
    		start();
    	}
    	int valueAddress = vm.insert(value);
    	//LOGGER.log(Level.INFO, "insert," + "address " + valueAddress + " key " + key + " thread " + Thread.currentThread().getId());
    	im.insert(key, valueAddress, keyAddress.get(), keyType.get());
    	if (!isTransaction) {
    		commit();
    	}
	}
	
	void update(Object key, DataBlock newValue, boolean isTransaction) {
		check(key);
		if (!isTransaction) {
			start();
    	}
		int address = im.search(key, keyType.get(), keyAddress.get());
		vm.update(address, newValue);
		if (!isTransaction) {
			commit();
    	}
	}
	
	DataBlock read(Object key, boolean isTransaction) {
		check(key);
		if (!isTransaction) {
			start();
    	}
		int address = im.search(key, keyType.get(), keyAddress.get());
		//LOGGER.log(Level.INFO, "read," + "address " + address + " key " + key + " thread " + Thread.currentThread().getId());
		DataBlock db = vm.read(address);
		if (!isTransaction) {
			commit();
    	}
		return db;
	}
	
	void delete(Object key, boolean isTransaction) {
		check(key);
		if (!isTransaction) {
			start();
    	}
		int address = im.search(key, keyType.get(), keyAddress.get());
		vm.delete(address);
		if (!isTransaction) {
			commit();
    	}
	}
	
	private void check(Object key) {
		if (tableName == null) {
			try {
				throw new NotSelectTableException();
			} catch (NotSelectTableException e) {
				LOGGER.log(Level.INFO, "未选择表");
			}
		}
		Type type = keyType.get();
		try {
			if (type == Type.int32) {
				if (!(key instanceof Integer)) {
					throw new ObjectMismatchException();
				}
			} else if (type == Type.long64) {
				if (!(key instanceof Long)) {
					throw new ObjectMismatchException();
				}
			} else if (type == Type.string64) {
				if (!(key instanceof String)) {
					throw new ObjectMismatchException();
				}
			} else {
				throw new ObjectMismatchException();
			}
		} catch (ObjectMismatchException e) {
			LOGGER.log(Level.INFO, "key mismatch your key:" + 
		    key.getClass().getSimpleName() + "key of table:" + type);
		}
		
	}
}
