package indexmanager;

import indexmanager.current.LockTable;

import java.util.ArrayDeque;

import tablemanager.Type;
import transactionManager.TransactionManager;
import util.DataUtil;
import datamanager.DataManager;
import datamanager.OutOfDiskSpaceException;


/*b+树
 * 存储基于dm
 * 
 * 放弃了,打算完全按照b+树并发协议做
 * */
class Tree {
	
	//锁表
	static LockTable lt = LockTable.getInstance();
	
	/*根节点地址
	 * */
	final int rootAddress; 	
	
	private Type type;
	
	Tree(int rootAddress, Type type) {
		this.type = type;
 		this.rootAddress = rootAddress;
	}
	
	//自动加读锁
	private Node getRootNode() {
		DataManager dm = DataManager.getInstance();
		int address = DataUtil.bytesToInt(dm.read(rootAddress), 0);
		lt.lockS(address);
		return Node.getNode(address);
	}
	
	//给出叶子节点时,叶子节点加着读锁
	private Node findLeafNode(Object key) {
		int i = 0;
		Node p = getRootNode();
		
        while (true) {
        	if (p.isLeaf()) {
				return p;
			}
        	
        	if (p.rightPos != -1 && IndexUtil.compareTo(key, p.rightFirstkey, type) >= 0) {
				int rightPos = p.rightPos;
				lt.unlockS(p.pos);
				lt.lockS(rightPos);
				p = p.getRight();
				i = 0;
				continue;
			}
        	
			while (i < p.n-1 && IndexUtil.compareTo(key, p.keys[i+1], type) >= 0) { 
				i++;
			}
			int nextPos = p.values[i];
			lt.unlockS(p.pos);
			lt.lockS(nextPos);
			p = p.getChild(nextPos);
			i = 0;
		}
	}
	
	int ssearch(Object key) {
		Node node = findLeafNode(key);
		if (IndexUtil.compareTo(key, node.keys[0], type) < 0) {
			lt.unlockS(node.pos);
			return -1;
		}
		int i = 0;
		while (i < node.n-1 && IndexUtil.compareTo(key, node.keys[i+1], type) >= 0) { 
			i++;
		}
		if (IndexUtil.compareTo(key, node.keys[i], type) == 0) {
			lt.unlockS(node.pos);
			return node.values[i];
		} 
		lt.unlockS(node.pos);
		return -1;
	}
	
	void iinsert(Object key, int value) throws IndexDuplicateException, OutOfDiskSpaceException {
		Node node = findLeafNode(key);
		lt.update(node.pos);
		while (true) {
			if (node.rightPos != -1 && IndexUtil.compareTo(key, node.rightFirstkey, type) >= 0) {
				lt.lockX(node.rightPos);
				lt.unlockX(node.pos);
				node = node.getRight();
			} else {
				break;
			}
		}
		if (node.isFull()) {
			split(node, key, value);
		} else {
			node.add(key, value);
			node.writeBackToDM();
			lt.unlockX(node.pos);
		}
	}
	
	//此时node加写锁
	private void split(Node node, Object key, int value) throws IndexDuplicateException, OutOfDiskSpaceException {
		while (node.isFull()) {
			Node parent = null;
			Node newNode = null;
			if (node.isRoot()) {
				parent = createNewRootNode(node);
				node.parentPos = parent.pos;
				newNode = createNewNode(node, key, value);
				updateRootAddress(parent.pos);
				key = newNode.keys[0];
				value = newNode.pos;
			} else {
				newNode = createNewNode(node, key, value);
				key = newNode.keys[0];
				value = newNode.pos;
				parent = getParentNode(node, key);
			}
			
			lt.unlockX(node.pos);
			node = parent;
		}
		node.add(key, value);
		node.writeBackToDM();
		lt.unlockX(node.pos);
	}
	
	private Node createNewRootNode(Node childNode) throws OutOfDiskSpaceException, IndexDuplicateException {
		Node newRoot = new Node(false, type);
		newRoot.add(childNode);
		newRoot.addToDM();
		return newRoot;
	}
	
	//更新根节点位置,同时给根节点加锁
	private void updateRootAddress(int newAddress) {
		byte[] bytes = new byte[4];
		DataUtil.intToBytes(newAddress, 0, bytes);
		DataManager.getInstance().update(rootAddress, bytes, TransactionManager.SUPER_ID);
	}
	
	//把node分裂,同时把key,value插入,此时node加锁
	private Node createNewNode(Node node, Object key, int value) throws OutOfDiskSpaceException, IndexDuplicateException {
		int mid = node.n/2;
		
		Node left = node;
		Node right = new Node(node.isLeaf(), type);
		right.setParentPos(node.parentPos);
		
		// mid --- node.n
		for (int j = 0; j < node.n-mid; j++) {
			right.add(node.keys[mid+j], node.values[mid+j]);
		}
		// 0 --- mid-1
		left.n = mid;
		
		//插入key,value
		if (IndexUtil.compareTo(key, right.keys[0], type) >= 0) {
			right.add(key, value);
		} else {
			left.add(key, value);
		}
		
		int rightPos = right.addToDM();
		left.setRight(rightPos, right.keys[0]);
		left.writeBackToDM();
		
		return right;
	}
	
	//对父节点加锁,可能要右移,包括右移
	private Node getParentNode(Node node, Object key) {
		lt.lockX(node.parentPos);
		Node pnode = Node.getNode(node.parentPos);
		while (pnode.rightPos != -1 && IndexUtil.compareTo(key, pnode.rightFirstkey, type) >= 0) {
			int rightPos = pnode.rightPos;
			lt.unlockX(pnode.pos);
			lt.lockX(rightPos);
			pnode = pnode.getRight();
			continue;
		}
		return pnode;
	}
	
	
	int search(Object key) {
		int i = 0;
		lt.lockS(rootAddress);
		Node p = Node.getNode(rootAddress);
		
		while (true) {
			
			if (IndexUtil.compareTo(key, p.keys[0], type) < 0) {
				lt.unlockS(p.pos);
				return -1;
			}
			
			while (i < p.n-1 && IndexUtil.compareTo(key, p.keys[i+1], type) >= 0) { 
				i++;
			}
			int nextPos = p.values[i];
			lt.unlockS(p.pos);
			
			if (p.rightPos != -1 && IndexUtil.compareTo(key, p.rightFirstkey, type) >= 0) {
				int rightPos = p.rightPos;
				lt.lockS(rightPos);
				p = p.getRight();
				i = 0;
				continue;
			}
			
			if (p.isLeaf()) {
				if (IndexUtil.compareTo(key, p.keys[i], type) == 0) {
					return p.values[i];
				} 
				return -1;
			}
			
			lt.lockS(nextPos);
			p = p.getChild(nextPos);
			i = 0;
		}
	}
	
	void insert(Object key, int value) throws OutOfDiskSpaceException, IndexDuplicateException {
		lt.lockS(rootAddress);
		Node node = Node.getNode(rootAddress);
		int i = 0;
		while (true) {
			if (lt.readToWrite(node.pos)) {
				//从dm刷新,因为没法做到锁升级
				node = Node.getNode(node.pos);
			}
			if (node.isFull()) {
				node= node.pos == rootAddress ? 
						rootSplit(node, key):
						nodeSplit(node, key);
			}
			
			if (node.isLeaf()) {
				if (lt.readToWrite(node.pos)) {
					//从dm刷新,因为没法做到锁升级
					node = Node.getNode(node.pos);
				}
				if (node.rightPos != -1 && IndexUtil.compareTo(key, node.rightFirstkey, type) >= 0) {
					int rightPos = node.rightPos;
					lt.unlockX(node.pos);
					lt.lockX(rightPos);
					node = node.getRight();
					i = 0;
					continue;
				} else {
					node.add(key, value);
					node.writeBackToDM();
					lt.unlockX(node.pos);
					return;
				}
			}
			
			if (lt.readToWrite(node.pos)) {
				//从dm刷新,因为没法做到锁升级
				node = Node.getNode(node.pos);
			}
			if (IndexUtil.compareTo(key, node.keys[0], type) < 0) {
				node.keys[0] = key;
				node.writeBackToDM();
			}
			lt.writeToRead(node.pos);//锁降级可以实现
			
			while (i < node.n-1 && IndexUtil.compareTo(key, node.keys[i+1], type) >= 0) { 
				i++;
			}
			
			int nextPos = node.values[i];
			lt.unlockS(node.pos);
			lt.lockS(nextPos);
			node = node.getChild(nextPos);
			i = 0;
		}
	}
	
	//node == root
	private Node rootSplit(Node node, Object key) throws OutOfDiskSpaceException, IndexDuplicateException {
		int mid = node.n/2;
		Object midKey = node.keys[mid];
		
		Node left = new Node(node.isLeaf(), type);
		left.setParentPos(node.pos);
		Node right = new Node(node.isLeaf(), type);
		right.setParentPos(node.pos);
		
		node.setIsLeaf(false);
		
		for (int j = 0; j < node.n-mid; j++) {
			right.add(node.keys[mid+j], node.values[mid+j]);
		}
		
		for (int j = 0; j < mid; j++) {
			left.add(node.keys[j], node.values[j]);
		}
		
		node.clear();
		int rightPos = right.addToDM();
		left.setRight(rightPos, right.keys[0]);
		int leftPos = left.addToDM();
		node.add(IndexUtil.compareTo(left.keys[0], key, type) < 0 ? left.keys[0] : key, leftPos);
		node.add(right.keys[0], rightPos);
		node.writeBackToDM();
		node = IndexUtil.compareTo(key, midKey, type) >= 0 ? right : left;
		lt.lockS(node.pos);
		lt.unlockX(rootAddress);
		
		return node;
	}
	
	//NonRoot
	private Node nodeSplit(Node node, Object key) throws OutOfDiskSpaceException, IndexDuplicateException {
		
		int mid = node.n/2;
		
		Object midKey = node.keys[mid];
		
		Node left = node;
		Node right = new Node(node.isLeaf(), type);
		right.setParentPos(node.parentPos);
		
		// mid --- node.n
		for (int j = 0; j < node.n-mid; j++) {
			right.add(node.keys[mid+j], node.values[mid+j]);
		}
		
		int rightPos = right.addToDM();
		
		// 0 --- mid-1
		left.n = mid;
		left.setRight(rightPos, right.keys[0]);
		left.writeBackToDM();
		lt.unlockX(left.pos);		
		lt.lockX(left.parentPos);
		Node father = Node.getNode(left.parentPos);
		father.add(right.keys[0], rightPos);
		father.writeBackToDM();
		lt.unlockX(left.parentPos);
		
		lt.lockS(left.pos);
		return IndexUtil.compareTo(key, midKey, type) >= 0 ? right : left;
	}
	
	
	
	@Override
	public String toString() {
		return Node.getNode(rootAddress).toTreeString();
	}
}
