package indexmanager;

import java.util.logging.Level;
import java.util.logging.Logger;

import tablemanager.Type;
import transactionManager.TransactionManager;
import util.pool.BlockPoolExecutor;
import util.pool.DataBlock;
import datamanager.DataManager;
import datamanager.OutOfDiskSpaceException;


/*b+树的一个节点
 * 
 * 增加右兄弟节点
 * */
class Node {
	
	// Logger
	private final static Logger LOGGER = Logger.getLogger(Node.class.getName());
	
	static final int LEAF_OFFSET = 0;
	static final int PARENT_POS_OFFSET = 1;
	static final int RIGHT_POS_OFFSET = 5;
	static final int N_OFFSET = 9;
	static final int VALUE_BASE_OFFSET = 13;
	final int KEY_BASE_OFFSET;
	final int RIGHT_FIRST_KEY_OFFSET;
	
	private DataBlock bytes = null;
	
	int pos = -1;
	
	final Type type;
	
	Node(boolean isLeaf, Type type) {
		this.type = type;
		KEY_BASE_OFFSET = VALUE_BASE_OFFSET + 2*Tree.BN*4;
		RIGHT_FIRST_KEY_OFFSET = KEY_BASE_OFFSET + 2*Tree.BN*type.getTypeLen();
		bytes = BlockPoolExecutor.getInstance().getDataBlock(getBytesLen());
		bytes.writeBoolean(LEAF_OFFSET, isLeaf);
		bytes.writeInt(PARENT_POS_OFFSET, -1);
		bytes.writeInt(RIGHT_POS_OFFSET, -1);
		bytes.writeInt(N_OFFSET, 0);
	}

	private Node(Type type) {
		this.type = type;
		KEY_BASE_OFFSET = VALUE_BASE_OFFSET + 2*Tree.BN*4;
		RIGHT_FIRST_KEY_OFFSET = VALUE_BASE_OFFSET + 2*Tree.BN*(4+type.getTypeLen());
	}

	void release() {
		bytes.release();
		bytes = null;
	}
	
	private void setBytes(DataBlock bytes) {
		this.bytes = bytes;
	}
	
	boolean isRoot() {
		return getParentPos() == -1;
	}
	
	int getParentPos() {
		return bytes.getInt(PARENT_POS_OFFSET);
	}
	
	int getRightPos() {
		return bytes.getInt(RIGHT_POS_OFFSET);
	}
	
	void setParentPos(int parentPos) {
		bytes.writeInt(PARENT_POS_OFFSET, parentPos);
	}
	
	void setRight(int rightPos, Object firstKey) {
		bytes.writeInt(RIGHT_POS_OFFSET, rightPos);
		setRightFirstKey(firstKey);
	}
	
	Object getRightFirstKey() {
		switch(getType()){
        case int32:
        	return bytes.getInt(RIGHT_FIRST_KEY_OFFSET);
        case long64:
        	return bytes.getLong(RIGHT_FIRST_KEY_OFFSET);
        default://type == Type.string64
        	return bytes.getString(RIGHT_FIRST_KEY_OFFSET);
        }
	}
	
	void setRightFirstKey(Object firstKey) {
		switch(getType()){
        case int32:
        	bytes.writeInt(RIGHT_FIRST_KEY_OFFSET, (Integer)firstKey);
        	break;
        case long64:
        	bytes.writeLong(RIGHT_FIRST_KEY_OFFSET, (Long)firstKey);
        	break;
        default://type == Type.string64
        	bytes.writeString(RIGHT_FIRST_KEY_OFFSET, (String)firstKey);
        	break;
        }
	}
	
	Type getType() {
		return type;
	}
	
	int getN() {
		return bytes.getInt(N_OFFSET);
	}
	
	void setN(int n) {
		bytes.writeInt(N_OFFSET, n);
	}
	
	Object getKey(int i) {
		Type type = getType();
		switch(type){
        case int32:
        	return bytes.getInt(KEY_BASE_OFFSET + i*type.getTypeLen());
        case long64:
        	return bytes.getLong(KEY_BASE_OFFSET + i*type.getTypeLen());
        default://type == Type.string64
        	return bytes.getString(KEY_BASE_OFFSET + i*type.getTypeLen());
        }
	}
	
	void setKey(int i, Object key) {
		switch(type){
        case int32:
        	bytes.writeInt(KEY_BASE_OFFSET + i*type.getTypeLen(), (Integer)key);
        	break;
        case long64:
        	bytes.writeLong(KEY_BASE_OFFSET + i*type.getTypeLen(), (Long)key);
        	break;
        default://type == Type.string64
        	bytes.writeString(KEY_BASE_OFFSET + i*type.getTypeLen(), (String)key);
        	break;
        }
	}
	
	int getValue(int i) {
		return bytes.getInt(VALUE_BASE_OFFSET + i*4);
	}
	
	void setValue(int i, int value) {
		bytes.writeInt(VALUE_BASE_OFFSET + i*4, value);
	}
	
	private void movesDown(int offset, int len) {
		if (len == 0) {
			return;
		}
		bytes.movesDown(VALUE_BASE_OFFSET + offset*4, 4, len*4);
		int typeLen = getType().getTypeLen();
		bytes.movesDown(KEY_BASE_OFFSET + offset*typeLen, typeLen, len*typeLen);
		/*
		System.arraycopy(bytes, VALUE_BASE_OFFSET + offset*4, bytes, 
				VALUE_BASE_OFFSET + (offset+1)*4, len*4);
		System.arraycopy(bytes, KEY_BASE_OFFSET + offset*typeLen, bytes, 
				KEY_BASE_OFFSET + (offset+1)*typeLen, len*typeLen);*/
	}

	boolean isLeaf() {
		return bytes.getBoolean(LEAF_OFFSET);
	}
	
	boolean isFull() {
		return getN() == 2*Tree.BN;
	}
	
	void add(Node node) {
		add(node.getKey(0), node.pos);
	}
	
	void add(final Object key, final int value) {
		int n = getN();
		int i = n;
		Type type = getType();
		while (i != 0 && IndexUtil.compareTo(getKey(i-1), key, type) > 0) {
			i--;
		}
		movesDown(i, n-i);
		
		//索引重复不被允许
		if (isLeaf() && i != 0 && IndexUtil.compareTo(key, getKey(i-1), type) == 0) {
			try {
				throw new IndexDuplicateException();
			} catch (IndexDuplicateException e) {
				LOGGER.log(Level.INFO, "key重复,重复key为:" + key);
			}
		}
		setKey(i, key);
		setValue(i, value);
		setN(++n);
	}
	
	void clear() {
		bytes.writeInt(N_OFFSET, 0);
	}
	
	void removeLast() {
		bytes.writeInt(N_OFFSET, bytes.getInt(N_OFFSET)-1);
	}
	
	void setIsLeaf(boolean isLeaf) {
		bytes.writeBoolean(LEAF_OFFSET, isLeaf);
	}
	
	int addToDM() throws OutOfDiskSpaceException {
		pos = DataManager.getInstance().insert(bytes, TransactionManager.SUPER_ID);
		return pos;
	}
	
	void writeBackToDM() {
		DataManager.getInstance().update(pos, bytes, TransactionManager.SUPER_ID);
	}
	
	private int getBytesLen() {
		return RIGHT_FIRST_KEY_OFFSET + type.getTypeLen();
	}
	
	static Node getNode(int address, Type type) {
		Node node = new Node(type);
		node.pos = address;
		node.setBytes(DataManager.getInstance().read(address));
		return node;
	}
	
	Node getChild(int address) {
		Node child = getNode(address, type);
		child.setParentPos(pos);
		return child;
	}
	
	Node getRight(int rightPos) {
		Node right = getNode(rightPos, type);
		//right.setParentPos(getParentPos());
		return right;
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer(isLeaf() + "");
		int n = getN();
		for (int i = 0; i < n; i++) {
			sb.append(" " + getKey(i));
		}
		sb.append('\n');
		return sb.toString();
	}
	
	public String toTreeString() {
		StringBuffer sb = new StringBuffer(this.toString());
		if (!this.isLeaf()) {
			int n = getN();
			for (int i = 0; i < n; i++) {
				sb.append(Node.getNode(getValue(i), type).toTreeString());
			}
		}
		return sb.toString();
	}
}
