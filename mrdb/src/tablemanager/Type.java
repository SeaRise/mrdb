package tablemanager;

import java.io.IOException;
import java.io.RandomAccessFile;

public enum Type {
	int32, //32位,4字节
	long64, //64位,8字节
	string64;//64个字符,utf-8是不定长编码,字节数很难控制,转换成char[]数组,一个char为两个字节.所以char数组字节数为128
	         //用不完的空间用0补齐
	         //再加上char[]长度4个字节,共132个字节
	
	static int INT32_LEN = 4;
	static int LONG64_LEN = 8;
	static int STRING64_LEN = 132;
	
	static int TYPE_LEN = 2;//Type设定为2字节
	
	public int getTypeLen() {
		if (this == Type.int32) {
			return INT32_LEN;
		} else if (this == Type.long64) {
			return LONG64_LEN;
		} else { // newNode.type == Type.string64
			return STRING64_LEN;
		}
	}
	
	public void writeToFile(RandomAccessFile file) throws IOException {
		byte[] type = new byte[2];
		if (this.equals(int32)) {
			type[0] = (byte)0;
			type[1] = (byte)0;
		} else if (this.equals(long64)) {
			type[0] = (byte)0;
			type[1] = (byte)1;
		} else {//string64
			type[0] = (byte)1;
			type[1] = (byte)0;
		}
		file.write(type);
	}
	
	public static Type readFromFile(RandomAccessFile file) throws IOException {
		byte[] type = new byte[2];
		type[0] = file.readByte();
		type[1] = file.readByte();
		if (type[0] == (byte)1) {
			return string64;
		} else { // type[0] == (byte)0
			if (type[1] == (byte)0) {
				return int32;
			} else { // type[1] == (byte)1
				return long64;
			}
		}
	}
}
