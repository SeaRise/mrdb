package datamanager.blockpool;

public class DataBlock {
	
	private final Slab slab;
	
	final byte[] bytes;
	
	DataBlock(int blockSize, Slab slab) {
		this.slab = slab;
		bytes = new byte[blockSize];
	}
	
	public void release() {
		slab.addBlock(this);
	}
}
