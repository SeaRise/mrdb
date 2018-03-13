package tablemanager;

import indexmanager.IndexDuplicateException;
import indexmanager.IndexManager;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

import transactionManager.TransactionManager;
import util.DataUtil;
import datamanager.OutOfDiskSpaceException;

public class TBMExecutor {
	
	private IndexManager im = new IndexManager();
	private TransactionManager tm = new TransactionManager();
	
	private RandomAccessFile tablesFile = null;
	
	Map<String, Table> tableMap = new HashMap<String, Table>();
	
	TBMExecutor() {
		try {
			tablesFile = new RandomAccessFile(TBMSetting.tablesFileName, "rw");
			while (tablesFile.getFilePointer() != tablesFile.length()) {
				Table t = Table.readFromFile(tablesFile);
				tableMap.put(t.tableName, t);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private long establishIndex(Type type) throws OutOfDiskSpaceException {
		return im.addRootNode(type);
	}
	
	private void checkTableExist(String tableName) throws TableNotFoundException {
		if (!tableMap.containsKey(tableName)) {
			throw new TableNotFoundException();
		}
	}
	
	//删除表的细节还需要考虑一下
	void createTable(String tableName, String[] fieldNames, 
			Type[] types, boolean[] isEstablishIndex) throws TableNameRepeatException, OutOfDiskSpaceException {
		
		if (tableMap.containsKey(tableName)) {
			throw new TableNameRepeatException();
		}
		
		Field[] fields = new Field[fieldNames.length];
		for (int i = 0; i < fields.length; i++) {
			fields[i] = new Field(fieldNames[i], types[i]);
			if (isEstablishIndex[i]) {
				fields[i].setBootAdress(establishIndex(types[i]));
			}
		}
		
		Table t = new Table(tableName, fields);
		tableMap.put(tableName, t);
		try {
			//把表数据更新到文件中
			t.addTable(tablesFile);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/*
	void deleteTable(String tableName) throws TableNotFoundException {
		checkTableExist(tableName);
		Table t = tableMap.get(tableName);
		t.isDroped = true;
		try {
			t.update(tablesFile);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}*/
	
	void start() {
		tm.startTransaction();
	}
	
	void commit() {
		tm.commitTransaction();
	}
	
	void abort() {
		tm.abortTransaction();
	}
	/*
	void insert(String tableName, DataType[] dataTypes, boolean isTransaction) throws TableNotFoundException, OutOfDiskSpaceException, IndexDuplicateException {
		checkTableExist(tableName);
		int address = tm.insert(packDataItem(dataTypes), isTransaction);
		Table t = tableMap.get(tableName);
		for (int i = 0; i < dataTypes.length; i++) {
			if (t.fields[i].isKey()) {
				im.insert(dataTypes[i].value, address, t.fields[i].bootAddress, dataTypes[i].type);
			}
		}
	}*/
	/*
	DataType[][] read(String tableName, DataType[] keyTypes, int[] keyIndexes) throws TableNotFoundException {
		checkTableExist(tableName);
		Table t = tableMap.get(tableName);
		
		long[] virtualAddresses = getAddress(t.fields, keyTypes, keyIndexes);
		if (virtualAddresses == null) {
			return null;
		}
		
		DataType[][] dataTypes = new DataType[virtualAddresses.length][];
		
		for (int i = 0; i < virtualAddresses.length && (virtualAddresses[i] != -1); i++) {
			byte[] dataItem = tm.read((int)virtualAddresses[i]);
			if (dataItem != null && dataItem.length != 0) {
				dataTypes[i] = parseDataItem(dataItem);
			} 
		}
		
		return dataTypes;
	}*/
	/*
	int update(String tableName, DataType[] keyTypes, int[] keyIndexes, 
			Object[] newValues, int[] valueIndexes, boolean isTransaction) throws TableNotFoundException {
		checkTableExist(tableName);
		
		Table t = tableMap.get(tableName);
		
		long[] virtualAddresses = getAddress(t.fields, keyTypes, keyIndexes);
		if (virtualAddresses == null) {
			return 0;
		}
		
		int updatedNum = virtualAddresses.length;
		
		DataType[][] dataTypes = new DataType[virtualAddresses.length][];
		
		for (int i = 0; i < virtualAddresses.length; i++) {
			dataTypes[i] = parseDataItem(tm.read((int)virtualAddresses[i]));//读
			
			//如果该记录已被删除.
			if (dataTypes[i] == null || (dataTypes[i].length == 0)) {
				updatedNum--;
				continue;
			}
			
			for (int j = 0; j < newValues.length; j++) {
				dataTypes[i][valueIndexes[j]].value = newValues[j]; //更新
			}
			
			tm.update((int)virtualAddresses[i], packDataItem(dataTypes[i]), isTransaction); //写
		}
		
		return updatedNum;
	}*/
	/*
	//根据各个索引得出的long[] 求出最终的交集,交集为long[],末尾为-1.
	private int getAddress(Field[] fields, DataType[] dataTypes, int[] indexes) {
		int i = 0;
		int addresses = null;
		for (; i < indexes.length; i++) {
			if (fields[indexes[i]].isKey() &&
			        (addresses = im.search(dataTypes[i].value, dataTypes[i].type, 
			        		fields[indexes[i]].bootAddress)) != null) {
				i++;
				break;
			}
		}
		int nextAddresses = null;
		for (; i < indexes.length; i++) {
			if (fields[indexes[i]].isKey() && 
					(nextAddresses = im.search(dataTypes[i].value, dataTypes[i].type, 
							fields[indexes[i]].bootAddress)) != null) {
				
				addresses = DataUtil.intersect(addresses, nextAddresses);
			}
		}
		return addresses;
	}*/
	
	
	//len + (types[i] + values[i]){0 to n-1}
	private DataType[] parseDataItem(byte[] dataItem) {
		int len = DataUtil.bytesToInt(dataItem, 0);
		
		DataType[] dataTypes = new DataType[len];
		
		int offset = Type.INT32_LEN;
		for (int i = 0; i < len; i++) {
			Type type = DataUtil.bytesToType(offset, dataItem);
			Object value = null;
			
			offset += Type.TYPE_LEN;
			if (type == Type.int32) {
				value = DataUtil.bytesToInt(dataItem, offset);
				offset += Type.INT32_LEN;
			} else if (type == Type.long64) {
				value = DataUtil.bytesToLong(dataItem, offset);
				offset += Type.LONG64_LEN;
			} else { // types[i] == Type.string64
				value = DataUtil.bytesToString(dataItem, offset);
				offset += Type.STRING64_LEN;
			}
			dataTypes[i] = new DataType(value, type);
		}
		
		return dataTypes;
	}
	
	//len + (types[i] + values[i]){0 to n-1}
	private byte[] packDataItem(DataType[] dataTypes) {
		byte[] dataItem = new byte[countDataItemLength(dataTypes)];
		DataUtil.intToBytes(dataTypes.length, 0, dataItem);
		int offset = Type.INT32_LEN;
		for (int i = 0; i < dataTypes.length; i++) {
			DataUtil.typeToBytes(dataTypes[i].type, offset, dataItem);
			offset += Type.TYPE_LEN;
			if (dataTypes[i].type == Type.int32) {
				DataUtil.intToBytes(((Integer)dataTypes[i].value).intValue(), offset, dataItem);
				offset += Type.INT32_LEN;
			} else if (dataTypes[i].type == Type.long64) {
				DataUtil.longToBytes(((Long)dataTypes[i].value).longValue(), offset, dataItem);
				offset += Type.LONG64_LEN;
			} else { // types[i] == Type.string64
				DataUtil.stringToBytes((String)dataTypes[i].value, offset, dataItem);
				offset += Type.STRING64_LEN;
			}
		}
		return dataItem;
	}
	
	
	private int countDataItemLength(DataType[] dataTypes) {
		int len = Type.INT32_LEN;
		for (DataType dt : dataTypes) {
			len += Type.TYPE_LEN;
			if (dt.type == Type.int32) {
				len += Type.INT32_LEN;
			} else if (dt.type == Type.long64) {
				len += Type.LONG64_LEN;
			} else { // type == Type.string64
				len += Type.STRING64_LEN;
			}
		}
		return len;
	}
}
