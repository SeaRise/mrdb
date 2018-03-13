package tablemanager;

import java.io.IOException;
import java.io.RandomAccessFile;

import util.DataUtil;

class Table {
	
	//不存在文件中,从文件读取时读取此时的偏移量载入.
	long address = -1;
	
	String tableName;
	/*
	//是否删除
	boolean isDroped = false;*/
	//int Nofields = fields.length
	Field[] fields;
	
	private Table(String tableName, Field[] fields, long address) {
		this.tableName = tableName;
		//this.isDroped = isDroped;
		this.fields = fields;
		this.address = address;
	}
	
	Table(String tableName, Field[] fields) {
		this.tableName = tableName;
		this.fields = fields;
	}
	
	void writeToFile(RandomAccessFile file) throws IOException {
		DataUtil.writeStringIntoFile(file, tableName);
		//file.writeBoolean(isDroped);
		file.writeInt(fields.length);
		for (Field f : fields) {
			f.writeToFile(file);
		}
	}
	
	void writeToFile(RandomAccessFile file, long offset) throws IOException {
		file.seek(offset);
		writeToFile(file);
	}
	
	void update(RandomAccessFile file) throws IOException {
		writeToFile(file, this.address);
	}
	
	void addTable(RandomAccessFile file) throws IOException {
		this.address = file.length();
		writeToFile(file, this.address);
	}
	
	static Table readFromFile(RandomAccessFile file) throws IOException {
		long address = file.getFilePointer();
		String tableName = DataUtil.readStringFromFile(file);
		//boolean isDroped = file.readBoolean();
		int len = file.readInt();
		Field[] fields = new Field[len];
		for (int i = 0; i < len; i++) {
			fields[i] = Field.readToFile(file);
		}
		return new Table(tableName, fields, address);
	}

	
	static Table readFromFile(RandomAccessFile file, int offset) throws IOException {
		file.seek(offset);
		return readFromFile(file);
	}
}
