package util.pool;

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
	
	public DataBlock doGetDataBlock(int blockLen) {
		//需要的字节大小小于32字节,就直接new,不分配了
		if (blockLen < BYTES_SIZE) {
			byte[][] bytess = new byte[1][];
			bytess[0] = new byte[blockLen];
			return new DataBlock(bytess, blockLen, this, false);
		}
		
		int num = blockLen / BYTES_SIZE;
		num = num*BYTES_SIZE < blockLen ? num+1 : num;
		
		byte[][] bytess = new byte[num][];
		int i = 0;
		for (;i < num && (bytess[i] = bytesList.poll()) != null; i++);
		for (;i < num; i++) {
			bytess[i] = new byte[BYTES_SIZE];
		}
		
		return new DataBlock(bytess, blockLen, this, true);
	}
	
	public DataBlock getDataBlock(int blockLen) {
		return doGetDataBlock(blockLen);
	}
	
	//外部申请的时候用这个方法
	public DataBlock getDataBlockMVCC(int blockLen) {
		//+8是为了存xmin和xmax.
		return doGetDataBlock(blockLen+8);
	}
}
