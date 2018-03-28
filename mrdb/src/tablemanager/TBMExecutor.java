package tablemanager;

import indexmanager.IndexDuplicateException;
import indexmanager.IndexManager;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import util.ParentPath;
import util.pool.DataBlock;
import versionmanager.VersionManager;
import datamanager.OutOfDiskSpaceException;

public class TBMExecutor {
	
	private static String TMB_END = "end";
	
	private IndexManager im = new IndexManager();
	private VersionManager vm = VersionManager.getInstance();
	
	private ThreadLocal<String> tableName = new ThreadLocal<String>();
	private ThreadLocal<Type> keyType = new ThreadLocal<Type>();
	private ThreadLocal<Integer> keyAddress = new ThreadLocal<Integer>();
	
	TBMExecutor() {
	}
	
	void createTable(String tableName, Type keyType) throws TableNameRepeatException, TableCreateFailExceotion {
		File file = new File(ParentPath.tablesFileParentName + tableName + ".t");
		try {
			doCreateTable(tableName, file, keyType);
		} catch (IOException | OutOfDiskSpaceException e) {
			file.delete();
			throw new TableCreateFailExceotion();
		}
	}
	
	private void doCreateTable(String tableName, File file, Type keyType) throws IOException, TableNameRepeatException, OutOfDiskSpaceException {	
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
			this.tableName.set(null);
			this.keyType.set(null);
			this.keyAddress.set(null);
			file.delete();
			throw new TableNotFoundException();
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
	
	void start() throws IOException {
		vm.startTransaction();
	}
	
	void commit() throws IOException {
		vm.commitTransaction();
	}
	
	void abort() throws IOException {
		vm.abortTransaction();
	}
	
    void insert(Object key, DataBlock value, boolean isTransaction) throws NotSelectTableException, ObjectMismatchException, OutOfDiskSpaceException, IndexDuplicateException {
    	check(key);
    	int valueAddress = vm.insert(value, isTransaction);
    	im.insert(key, valueAddress, keyAddress.get(), keyType.get());
	}
	
	void update(Object key, DataBlock newValue, boolean isTransaction) throws NotSelectTableException, ObjectMismatchException {
		check(key);
		int address = im.search(key, keyType.get(), keyAddress.get());
		vm.update(address, newValue, isTransaction);
	}
	
	DataBlock read(Object key) throws NotSelectTableException, ObjectMismatchException {
		check(key);
		int address = im.search(key, keyType.get(), keyAddress.get());
		return vm.read(address);
	}
	
	private void check(Object key) throws NotSelectTableException, ObjectMismatchException {
		if (tableName == null) {
			throw new NotSelectTableException();
		}
		Type type = keyType.get();
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
	}
}
