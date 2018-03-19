package datamanager.blockpool;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ConcurrentSkipListMap;


public class BlockPoolExecutor {
	
	static final int SLAB_SIZE = 1<<20;
	
	static final int FIRST_BLOCK_SIZE = 128;
	
	final private ConcurrentSkipListMap<Integer, Slab> slabs;
	
	final private Queue<Slab> slabQueue;
	
	private final int initSize;
	
	private BlockPoolExecutor(int initSize) {
		this.initSize = initSize;
		slabs = new ConcurrentSkipListMap<Integer, Slab>();
		slabQueue = new LinkedList<Slab>();
	}
	
	DataBlock getDataBlock(int needLen) {
		int blockLen = FIRST_BLOCK_SIZE;
		while (needLen > blockLen) {
			blockLen >>= 1;
	        //超过1024就.........
		}
		
		Slab slab = null;
		
		
		if (slabs.size() < initSize) {
			
		}
		
		return null;
	}
}
