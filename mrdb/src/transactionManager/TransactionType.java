package transactionManager;

import java.io.IOException;
import java.io.RandomAccessFile;

public enum TransactionType {
	start, //事务开始
	active,//事务进行
	commit, //事务提交
	abort;//事务回滚
	
	static int TRANSACTION_TYPE_LEN = 2;//Type设定为2字节
	
	
	public void writeToFile(RandomAccessFile file) throws IOException {
		byte[] type = new byte[2];
		if (this.equals(start)) {
			type[0] = (byte)0;
			type[1] = (byte)0;
		} else if (this.equals(commit)) {
			type[0] = (byte)0;
			type[1] = (byte)1;
		} else if (this.equals(abort)){
			type[0] = (byte)1;
			type[1] = (byte)0;
		} else {//active
			type[0] = (byte)1;
			type[1] = (byte)1;
		}
		file.write(type);
	}
	
	public static TransactionType readFromFile(RandomAccessFile file) throws IOException {
		byte[] type = new byte[2];
		type[0] = file.readByte();
		type[1] = file.readByte();
		if (type[0] == (byte)1) {
			if (type[1] == (byte)0) {
				return TransactionType.abort;
			} else { // type[1] == (byte)1
				return TransactionType.active;
			}
		} else { // type[0] == (byte)0
			if (type[1] == (byte)0) {
				return TransactionType.start;
			} else { // type[1] == (byte)1
				return TransactionType.commit;
			}
		}
	}
}
