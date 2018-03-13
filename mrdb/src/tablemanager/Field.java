package tablemanager;

import java.io.IOException;
import java.io.RandomAccessFile;

import util.DataUtil;

class Field {
	
	String fieldName;
	Type type;
	long bootAddress = -1;//索引的根结点的地址
	
	private Field(String fieldName, Type type, long bootAddress) {
		this.fieldName = fieldName;
		this.type = type;
		this.bootAddress = bootAddress;
	}
	
	Field(String fieldName, Type type) {
		this.fieldName = fieldName;
		this.type = type;
	}
	
	boolean isKey() {
		return this.bootAddress != -1;
	}
	
	void setBootAdress(long address) {
		bootAddress = address;
	}
	
	void writeToFile(RandomAccessFile file) throws IOException {
		DataUtil.writeStringIntoFile(file, fieldName);
		type.writeToFile(file);
		file.writeLong(bootAddress);
	}
	
	static Field readToFile(RandomAccessFile file) throws IOException {
		String fieldName = DataUtil.readStringFromFile(file);
		Type type = Type.readFromFile(file);
		long bootAddress = file.readLong();
		return new Field(fieldName, type, bootAddress);
	}
}
