package com.mrdb.im;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.mrdb.dm.DataManager;
import com.mrdb.dm.exception.OutOfDiskSpaceException;
import com.mrdb.tbm.Type;
import com.mrdb.tm.TransactionManager;
import com.mrdb.util.pool.BlockPoolExecutor;
import com.mrdb.util.pool.DataBlock;

public class IndexManager {
	
	//private Tree tree = null;
	private ThreadLocal<Tree> tree = new ThreadLocal<Tree>();
	
	// Logger
	private final static Logger LOGGER = Logger.getLogger(IndexManager.class.getName());
	
	public IndexManager() {
		
	}
	
	private void selectTree(int rootAddress, Type type) {
		if (tree.get() == null || tree.get().rootAddress != rootAddress) {
			tree.set(new Tree(rootAddress, type));
		}
	}
	
	public void insert(Object key, int address, int rootAddress, Type type) {
		selectTree(rootAddress, type);
		tree.get().iinsert(key, address);
	}
	
	public int search(Object key, Type type, int rootAddress) {
		selectTree(rootAddress, type);
		return tree.get().ssearch(key);
		
	}

	public int addRootNode(Type type) {
		int address = -1;
		int rootAddress = -1;
		try {
			address = new Node(true, type).addToDM();
			DataBlock block = BlockPoolExecutor.getInstance().getDataBlock(4);
			block.writeInt(0, address);
			rootAddress = DataManager.getInstance().insert(block, TransactionManager.SUPER_ID);
		} catch (OutOfDiskSpaceException e) {
			LOGGER.log(Level.INFO, "建立索引失败,空间不足");
		}
		
		return rootAddress;
	}

	@Override
	public String toString() {
		if (tree.get() == null) {
			return null;
		}
		return tree.get().toString();
	}
}
