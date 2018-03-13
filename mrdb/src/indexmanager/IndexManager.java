package indexmanager;

import tablemanager.Type;
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
		tree.get().insert(key, address);
	}
	
	public int search(Object key, Type type, int rootAddress) {
		selectTree(rootAddress, type);
		return tree.get().search(key);
		
	}

	public int addRootNode(Type type) throws OutOfDiskSpaceException {
		return new Node(true, type).addToDM();
	}

	@Override
	public String toString() {
		if (tree.get() == null) {
			return null;
		}
		return tree.get().toString();
	}
	
	
	
}
