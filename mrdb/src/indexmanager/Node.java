package indexmanager;

import tablemanager.Type;
import transactionManager.TransactionManager;
import util.DataUtil;
import datamanager.DataManager;
import datamanager.OutOfDiskSpaceException;


/*b+树的一个节点
 * 
 * 增加右兄弟节点
 * */
class Node {
	
	static final int LEAF_OFFSET = 0;
	static final int PARENT_POS_OFFSET = 1;
	static final int RIGHT_POS_OFFSET = 5;
	static final int N_OFFSET = 9;
	static final int VALUE_BASE_OFFSET = 13;
	final int KEY_BASE_OFFSET;
	final int RIGHT_FIRST_KEY_OFFSET;
	
	private byte[] bytes = null;
	
	int pos = -1;
	
	final Type type;
	
	Node(boolean isLeaf, Type type) {
		this.type = type;
		KEY_BASE_OFFSET = VALUE_BASE_OFFSET + 2*IMSetting.BN*4;
		RIGHT_FIRST_KEY_OFFSET = KEY_BASE_OFFSET + 2*IMSetting.BN*type.getTypeLen();
		bytes = new byte[getBytesLen()];
		DataUtil.booleanToBytes(isLeaf, LEAF_OFFSET, bytes);
		DataUtil.intToBytes(-1, PARENT_POS_OFFSET, bytes);
		DataUtil.intToBytes(-1, RIGHT_POS_OFFSET, bytes);
		DataUtil.intToBytes(0, N_OFFSET, bytes);
	}
	
	private Node(Type type) {
		this.type = type;
		KEY_BASE_OFFSET = VALUE_BASE_OFFSET + 2*IMSetting.BN*4;
		RIGHT_FIRST_KEY_OFFSET = VALUE_BASE_OFFSET + 2*IMSetting.BN*(4+type.getTypeLen());
	}
	
	private void setBytes(byte[] bytes) {
		this.bytes = bytes;
	}
	
	boolean isRoot() {
		return getParentPos() == -1;
	}
	
	int getParentPos() {
		return DataUtil.bytesToInt(bytes, PARENT_POS_OFFSET);
	}
	
	int getRightPos() {
		return DataUtil.bytesToInt(bytes, RIGHT_POS_OFFSET);
	}
	
	void setParentPos(int parentPos) {
		DataUtil.intToBytes(parentPos, PARENT_POS_OFFSET, bytes);
	}
	
	void setRight(int rightPos, Object firstKey) {
		DataUtil.intToBytes(rightPos, RIGHT_POS_OFFSET, bytes);
		setRightFirstKey(firstKey);
	}
	
	Object getRightFirstKey() {
		switch(getType()){
        case int32:
        	return DataUtil.bytesToInt(bytes, RIGHT_FIRST_KEY_OFFSET);
        case long64:
        	return DataUtil.bytesToLong(bytes, RIGHT_FIRST_KEY_OFFSET);
        default://type == Type.string64
        	return DataUtil.bytesToString(bytes, RIGHT_FIRST_KEY_OFFSET);
        }
	}
	
	void setRightFirstKey(Object firstKey) {
		switch(getType()){
        case int32:
        	DataUtil.intToBytes((Integer)firstKey, RIGHT_FIRST_KEY_OFFSET, bytes);
        	break;
        case long64:
        	DataUtil.longToBytes((Long)firstKey, RIGHT_FIRST_KEY_OFFSET, bytes);
        	break;
        default://type == Type.string64
        	DataUtil.stringToBytes((String)firstKey, RIGHT_FIRST_KEY_OFFSET, bytes);
        	break;
        }
	}
	
	Type getType() {
		return type;
	}
	
	int getN() {
		return DataUtil.bytesToInt(bytes, N_OFFSET);
	}
	
	void setN(int n) {
		DataUtil.intToBytes(n, N_OFFSET, bytes);
	}
	
	Object getKey(int i) {
		Type type = getType();
		switch(type){
        case int32:
        	return DataUtil.bytesToInt(bytes, KEY_BASE_OFFSET + i*type.getTypeLen());
        case long64:
        	return DataUtil.bytesToLong(bytes, KEY_BASE_OFFSET + i*type.getTypeLen());
        default://type == Type.string64
        	return DataUtil.bytesToString(bytes, KEY_BASE_OFFSET + i*type.getTypeLen());
        }
	}
	
	void setKey(int i, Object key) {
		switch(type){
        case int32:
        	DataUtil.intToBytes((Integer)key, KEY_BASE_OFFSET + i*type.getTypeLen(), bytes);
        	break;
        case long64:
        	DataUtil.longToBytes((Long)key, KEY_BASE_OFFSET + i*type.getTypeLen(), bytes);
        	break;
        default://type == Type.string64
        	DataUtil.stringToBytes((String)key, KEY_BASE_OFFSET + i*type.getTypeLen(), bytes);
        	break;
        }
	}
	
	int getValue(int i) {
		return DataUtil.bytesToInt(bytes, VALUE_BASE_OFFSET + i*4);
	}
	
	void setValue(int i, int value) {
		DataUtil.intToBytes(value, VALUE_BASE_OFFSET + i*4, bytes);
	}
	
	private void movesDown(int offset, int len) {
		System.arraycopy(bytes, VALUE_BASE_OFFSET + offset*4, bytes, 
				VALUE_BASE_OFFSET + (offset+1)*4, len*4);
		int typeLen = getType().getTypeLen();
		System.arraycopy(bytes, KEY_BASE_OFFSET + offset*typeLen, bytes, 
				KEY_BASE_OFFSET + (offset+1)*typeLen, len*typeLen);
	}

	boolean isLeaf() {
		return DataUtil.bytesToBoolean(bytes, LEAF_OFFSET);
	}
	
	boolean isFull() {
		return getN() == 2*IMSetting.BN;
	}
	
	void add(Node node) throws IndexDuplicateException {
		add(node.getKey(0), node.pos);
	}
	
	void add(final Object key, final int value) throws IndexDuplicateException {
		int n = getN();
		int i = n;
		Type type = getType();
		while (i != 0 && IndexUtil.compareTo(getKey(i-1), key, type) > 0) {
			i--;
		}
		movesDown(i, n-i);
		
		//索引重复不被允许
		if (isLeaf() && i != 0 && IndexUtil.compareTo(key, getKey(i-1), type) == 0) {
			throw new IndexDuplicateException();
		}
		setKey(i, key);
		setValue(i, value);
		setN(++n);
	}
	
	void clear() {
		DataUtil.intToBytes(0, N_OFFSET, bytes);
	}
	
	void removeLast() {
		DataUtil.intToBytes(DataUtil.bytesToInt(bytes, N_OFFSET)-1, N_OFFSET, bytes);
	}
	
	void setIsLeaf(boolean isLeaf) {
		DataUtil.booleanToBytes(isLeaf, LEAF_OFFSET, bytes);
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
	
	Node getRight() {
		Node right = getNode(getRightPos(), type);
		right.setParentPos(getParentPos());
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
