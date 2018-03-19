package datamanager.pool;

import java.util.concurrent.ConcurrentLinkedQueue;



public class BlockPoolExecutor {
	
	static final int BYTES_SIZE = 32;
	
	static final int MAX_BYTES_NUM = 5000;
	
	final ConcurrentLinkedQueue<byte[]> bytesList;
	
	BlockPoolExecutor() {
		bytesList = new ConcurrentLinkedQueue<byte[]>();
	}
	
	boolean addBytes(byte[] bytes) {
		if (bytesList.size() == MAX_BYTES_NUM) {
			return false;
		}
		bytesList.offer(bytes);
		return true;
	}
	
	DataBlock getDataBlock(int blockLen) {
		int num = blockLen / BYTES_SIZE;
		num = num*32 < blockLen ? num+1 : num;
		
		byte[][] bytess = new byte[num][];
		int i = 0;
		for (;i < num && (bytess[i] = bytesList.poll()) != null; i++);
		for (;i < num; i++) {
			bytess[i] = new byte[BYTES_SIZE];
		}
		
		return new DataBlock(bytess, this);
	}
}
