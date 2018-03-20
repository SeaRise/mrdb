package indexmanager;

import indexmanager.current.LockTable;

import java.util.ArrayDeque;

import tablemanager.Type;
import transactionManager.TransactionManager;
import util.DataUtil;
import datamanager.DataManager;
import datamanager.OutOfDiskSpaceException;
import datamanager.pool.BlockPoolExecutor;
import datamanager.pool.DataBlock;


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
		int address = dm.read(rootAddress).getInt(0);
		lt.lockS(address);
		return Node.getNode(address, type);
	}
	
	private Node getRoot() {
		DataManager dm = DataManager.getInstance();
		int address = dm.read(rootAddress).getInt(0);
		return Node.getNode(address, type);
	}
	
	//给出叶子节点时,叶子节点加着锁
	private Node findLeafNode(Object key, boolean onlyRead) {
		int i = 0;
		Node p = getRootNode();
		
        while (true) {
        	if (p.isLeaf()) {
        		if (!onlyRead) {
        			lt.unlockS(p.pos);
        			lt.lockX(p.pos);
        			p = Node.getNode(p.pos, type);
        		}
				return p;
			}
        	
        	int rightPos = p.getRightPos();
        	if (rightPos != -1 && IndexUtil.compareTo(key, p.getRightFirstKey(), type) >= 0) {
	        	lt.unlockS(p.pos);
				lt.lockS(rightPos);
				p = p.getRight();
				continue;
			}
        	
        	int n = p.getN();
			while (i < n-1 && IndexUtil.compareTo(key, p.getKey(i+1), type) >= 0) { 
				i++;
			}
			int nextPos = p.getValue(i);
			lt.unlockS(p.pos);
			lt.lockS(nextPos);
			p = p.getChild(nextPos);
			i = 0;
		}
	}
	
	int ssearch(Object key) {
		Node node = findLeafNode(key, true);
		if (node.getN() == 0 || IndexUtil.compareTo(key, node.getKey(0), type) < 0) {
			lt.unlockS(node.pos);
			return -1;
		}
		int i = 0;
		while (true) {
			int rightPos = node.getRightPos();
			if (rightPos != -1 && IndexUtil.compareTo(key, node.getRightFirstKey(), type) >= 0) {
				lt.unlockS(node.pos);
				lt.lockS(rightPos);
				node = node.getRight();
				continue;
			}
			
			int n = node.getN();
			while (i < n-1 && IndexUtil.compareTo(key, node.getKey(i+1), type) >= 0) { 
				i++;
			}
			if (IndexUtil.compareTo(key, node.getKey(i), type) == 0) {
				lt.unlockS(node.pos);
				return node.getValue(i);
			} 
			lt.unlockS(node.pos);
			return -1;
		}
	}
	
	void iinsert(Object key, int value) throws IndexDuplicateException, OutOfDiskSpaceException {
		Node node = findLeafNode(key, false);
		
		while (true) {
			int rightPos = node.getRightPos();
			if (rightPos != -1 && IndexUtil.compareTo(key, node.getRightFirstKey(), type) >= 0) {
				lt.unlockX(node.pos);
				lt.lockX(rightPos);
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
		Node parent = null;
		final Object kkey = key;
		while (node.isFull()) {
			Node newNode = null;
			if (node.isRoot()) {
				//此时parent加锁,node加锁
				parent = createNewRootNode(node, kkey);
				node.setParentPos(parent.pos);
				newNode = createNewNode(node, key, value);
				updateRootAddress(parent.pos);
				key = newNode.getKey(0);
				value = newNode.pos;
			} else {
				newNode = createNewNode(node, key, value);
				key = newNode.getKey(0);
				value = newNode.pos;
				parent = getParentNode(node, key);
				Object pkey = parent.getKey(0);
				parent.setKey(0, IndexUtil.compareTo(kkey, pkey, type) < 0 ? kkey : pkey);
			}
			
			lt.unlockX(node.pos);
			node = parent;
		}
		node.add(key, value);
		node.writeBackToDM();
		
		while (!node.isRoot()) {
			parent = getParentNode(node, key);
			Object pkey = parent.getKey(0);
			parent.setKey(0, IndexUtil.compareTo(kkey, pkey, type) < 0 ? kkey : pkey);
			parent.writeBackToDM();
			lt.unlockX(node.pos);
			node = parent;
		}
		
		lt.unlockX(node.pos);
	}
	
	//创建新的根节点,同时加锁
	private Node createNewRootNode(Node childNode, final Object kkey) throws OutOfDiskSpaceException, IndexDuplicateException {
		Node newRoot = new Node(false, type);
		Object ckey = childNode.getKey(0);
		newRoot.add(IndexUtil.compareTo(kkey, ckey, type) < 0 ? kkey : ckey, childNode.pos);
		lt.lockX(newRoot.addToDM());
		return newRoot;
	}
	
	//更新根节点位置
	private void updateRootAddress(int newAddress) {
		DataBlock block = BlockPoolExecutor.getInstance().getDataBlock(4);
		block.writeInt(0, newAddress);
		DataManager.getInstance().update(rootAddress, block, TransactionManager.SUPER_ID);
	}
	
	//把node分裂,同时把key,value插入,此时node加锁
	private Node createNewNode(Node node, Object key, int value) throws OutOfDiskSpaceException, IndexDuplicateException {
		int n = node.getN();
		int mid = n/2;
		
		Node left = node;
		Node right = new Node(node.isLeaf(), type);
		right.setParentPos(node.getParentPos());
		
		// mid --- node.n
		for (int j = 0; j < n-mid; j++) {
			right.add(node.getKey(mid+j), node.getValue(mid+j));
		}
		// 0 --- mid-1
		left.setN(mid);
		
		//插入key,value
		if (IndexUtil.compareTo(key, right.getKey(0), type) >= 0) {
			right.add(key, value);
		} else {
			left.add(key, value);
		}
		
		int rightPos = right.addToDM();
		left.setRight(rightPos, right.getKey(0));
		left.writeBackToDM();
		
		return right;
	}
	
	//对父节点加锁,可能要右移,包括右移
	private Node getParentNode(Node node, Object key) {
		int parentPos = node.getParentPos();
		lt.lockX(parentPos);
		Node pnode = Node.getNode(parentPos, type);
		int rightPos;
		while ((rightPos = pnode.getRightPos()) != -1 && IndexUtil.compareTo(key, pnode.getRightFirstKey(), type) >= 0) {
			lt.unlockX(pnode.pos);
			lt.lockX(rightPos);
			pnode = pnode.getRight();
			continue;
		}
		return pnode;
	}
	
	@Override
	public String toString() {
		return getRoot().toTreeString();
	}
}
