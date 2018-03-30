package indexmanager;

import indexmanager.current.LockTable;
import tablemanager.Type;
import transactionManager.TransactionManager;
import util.pool.BlockPoolExecutor;
import util.pool.DataBlock;
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
	
	final static int BN = 3; 
	
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
        			p.release();
        			lt.unlockS(p.pos);
        			lt.lockX(p.pos);
        			//此时p加写锁
        			p = Node.getNode(p.pos, type);
        		}
        		//search加读锁,insert加写锁
				return p;
			}
        	
        	//p不是叶子节点.由条件可知,只有最右边的节点可能调用这个方法.
        	updateMinKey(p, onlyRead, key);
        	
        	int rightPos;
        	while ((rightPos = p.getRightPos()) != -1 && IndexUtil.compareTo(key, p.getRightFirstKey(), type) >= 0) {
        		p.release();
				lt.lockS(rightPos);
				lt.unlockS(p.pos);
				p = p.getRight(rightPos);
			}
        	
        	int n = p.getN();
			while (i < n-1 && IndexUtil.compareTo(key, p.getKey(i+1), type) >= 0) { 
				i++;
			}
			int nextPos = p.getValue(i);
			p.release();
			lt.unlockS(p.pos);
			lt.lockS(nextPos);
			p = p.getChild(nextPos);
			i = 0;
		}
	}
	
	//此时加着读锁
	//双重检查
	//返回时node加读锁
	private void updateMinKey(Node node, boolean onlyRead, Object key) {
		if (onlyRead) {
			return;
		}
		
		if (IndexUtil.compareTo(key, node.getKey(0), type) < 0 ) {
			lt.unlockS(node.pos);
			lt.lockX(node.pos);
			node.release();
			node = Node.getNode(node.pos, type);
			if (IndexUtil.compareTo(key, node.getKey(0), type) < 0) {
				node.setKey(0, key);
			    node.writeBackToDM();
			}
			//锁降级不会报错
			lt.lockS(node.pos);
			lt.unlockX(node.pos);
		}
	}
	
	
	
	int ssearch(Object key) {
		Node node = findLeafNode(key, true);
		if (node.getN() == 0 || IndexUtil.compareTo(key, node.getKey(0), type) < 0) {
			node.release();
			lt.unlockS(node.pos);
			return -1;
		}
		int i = 0;
		while (true) {
			int rightPos = node.getRightPos();
			if (rightPos != -1 && IndexUtil.compareTo(key, node.getRightFirstKey(), type) >= 0) {
				node.release();
				lt.lockS(rightPos);
				lt.unlockS(node.pos);
				node = node.getRight(rightPos);
				continue;
			}
			
			int n = node.getN();
			while (i < n-1 && IndexUtil.compareTo(key, node.getKey(i+1), type) >= 0) { 
				i++;
			}
			if (IndexUtil.compareTo(key, node.getKey(i), type) == 0) {
				int value = node.getValue(i);
				node.release();
				lt.unlockS(node.pos);
				return value;
			} 
			node.release();
			lt.unlockS(node.pos);
			return -1;
		}
	}
	
	void iinsert(Object key, int value) throws IndexDuplicateException, OutOfDiskSpaceException {
		//此时node加写锁
		Node node = findLeafNode(key, false);
		
		int rightPos;
		while ((rightPos = node.getRightPos()) != -1 && IndexUtil.compareTo(key, node.getRightFirstKey(), type) > 0) {
			node.release();
			lt.lockX(rightPos);
			lt.unlockX(node.pos);
			node = node.getRight(rightPos);
		}
		
		//此时node加写锁
		if (node.isFull()) {
			split(node, key, value);
		} else {
			node.add(key, value);
			node.writeBackToDM();
			node.release();
			lt.unlockX(node.pos);
		}
	}
	
	//此时node加写锁
	private void split(Node node, Object key, int value) throws IndexDuplicateException, OutOfDiskSpaceException {
		Node parent = null;
		//final Object kkey = key;
		while (node.isFull()) {
			Node newNode = null;
			if (node.isRoot()) {
				//node加着写锁,parent加写锁,且parent已经写入dm中了.
				parent = createNewRootNode(node);
				node.setParentPos(parent.pos);
				//这一行在dm中更新了node和newNode,所以在dm中,此时node的父节点是parent.newNode的父节点也是parent
				newNode = createNewNode(node, key, value);
				//这是再更新根节点位置.
			    updateRootAddress(parent.pos);
				key = newNode.getKey(0);
				value = newNode.pos;
				newNode.release();
			} else {
				//node加写锁
				newNode = createNewNode(node, key, value);
				key = newNode.getKey(0);
				value = newNode.pos;
				newNode.release();
				//此时parent加写锁
				parent = getParentNode(node, key);
			}
			
			node.release();
			lt.unlockX(node.pos);
			node = parent;
		}
		node.add(key, value);
		node.writeBackToDM();
		
		node.release();
		lt.unlockX(node.pos);
	}
	
	//创建新的根节点,同时加锁
	private Node createNewRootNode(Node childNode) throws OutOfDiskSpaceException, IndexDuplicateException {
		Node newRoot = new Node(false, type);
		Object ckey = childNode.getKey(0);
		//newRoot.add(IndexUtil.compareTo(kkey, ckey, type) < 0 ? kkey : ckey, childNode.pos);
		newRoot.add(ckey, childNode.pos);
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
			lt.lockX(rightPos);
			lt.unlockX(pnode.pos);
			pnode.release();
			pnode = pnode.getRight(rightPos);
		}
		//此时pnode加写锁
		return pnode;
	}
	
	@Override
	public String toString() {
		return getRoot().toTreeString();
	}
}
