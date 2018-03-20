package datamanager.pool;

import java.util.concurrent.ConcurrentLinkedQueue;



public class BlockPoolExecutor {
	
	private static BlockPoolExecutor executor = new BlockPoolExecutor();
	
	public static final int BYTES_SIZE = 32;
	
	static final int MAX_BYTES_NUM = 5000;
	
	final ConcurrentLinkedQueue<byte[]> bytesList;
	
	public static BlockPoolExecutor getInstance() {
		return executor;
	}
	
	private BlockPoolExecutor() {
		bytesList = new ConcurrentLinkedQueue<byte[]>();
	}
	
	boolean addBytes(byte[] bytes) {
		if (bytesList.size() == MAX_BYTES_NUM) {
			return false;
		}
		bytesList.offer(bytes);
		return true;
	}
	
	public DataBlock getDataBlock(int blockLen) {
		//System.out.println("get" + blockLen);
		int num = blockLen / BYTES_SIZE;
		num = num*BYTES_SIZE < blockLen ? num+1 : num;
		
		byte[][] bytess = new byte[num][];
		int i = 0;
		for (;i < num && (bytess[i] = bytesList.poll()) != null; i++);
		for (;i < num; i++) {
			bytess[i] = new byte[BYTES_SIZE];
		}
		
		return new DataBlock(bytess, blockLen, this);
	}
}