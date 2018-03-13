package indexmanager;

import tablemanager.Type;
import transactionManager.TransactionManager;
import util.DataUtil;
import datamanager.DataManager;
import datamanager.OutOfDiskSpaceException;

public class IndexManager {
	
	//private Tree tree = null;
	private ThreadLocal<Tree> tree = new ThreadLocal<Tree>();
	
	public IndexManager() {
		
	}
	
	private void selectTree(int rootAddress, Type type) {
		if (tree.get() == null || tree.get().rootAddress != rootAddress) {
			tree.set(new Tree(rootAddress, type));
		}
	}
	
	public void insert(Object key, int address, int rootAddress, Type type) throws OutOfDiskSpaceException, IndexDuplicateException {
		selectTree(rootAddress, type);
		tree.get().iinsert(key, address);
	}
	
	public int search(Object key, Type type, int rootAddress) {
		selectTree(rootAddress, type);
		return tree.get().ssearch(key);
		
	}

	public int addRootNode(Type type) throws OutOfDiskSpaceException {
		int address = new Node(true, type).addToDM();
		byte[] bytes = new byte[4];
		DataUtil.intToBytes(address, 0, bytes);
		return DataManager.getInstance().insert(bytes, TransactionManager.SUPER_ID);
	}

	@Override
	public String toString() {
		if (tree.get() == null) {
			return null;
		}
		return tree.get().toString();
	}
	
	
	
}
