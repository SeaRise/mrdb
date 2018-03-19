package datamanager.blockpool;

import java.util.LinkedList;

public class Slab {
	
	//空闲的块 列表
	final LinkedList<DataBlock> blockList = new LinkedList<DataBlock>();
	
	final private SlabGroup group;
	
	boolean used = false;
	
	Slab(int blockSize, SlabGroup group) {
		this.group = group;
		int num = BlockPoolExecutor.SLAB_SIZE / blockSize;
		for (int i = 0; i < num; i++) {
			blockList.add(new DataBlock(blockSize, this));
		}
	}
	
	void addBlock(DataBlock block) {
		blockList.add(block);
	}
	
	//null or block
	DataBlock getBlock() {
		used = true;
		return blockList.poll();
	}
	
	boolean hasFreeBlock() {
		return !blockList.isEmpty();
	}
}
