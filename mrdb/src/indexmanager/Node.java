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
	
	Type type;
	
	boolean isLeaf;
	
	int pos = -1;
	
	int parentPos = -1;
	
	int rightPos = -1;
	
	Object[] keys = null;
	int[] values = null;
	
	//右兄弟的第一个key值,用于判断是否右移,没右兄弟为null
	Object rightFirstkey = null;
	
	/*现存关键字的数目
	 * */
	int n = 0;
	
	Node(boolean isLeaf, Type type) {
		this.isLeaf = isLeaf;
		this.type = type;
		initKeyAndValues();
	}
	
	boolean isRoot() {
		return parentPos == -1;
	}
	
	void setParentPos(int parentPos) {
		this.parentPos = parentPos;
	}
	
	void setRight(int rightPos, Object firstKey) {
		this.rightPos = rightPos;
		this.rightFirstkey = firstKey;
	}
	
	static Node getNode(int address) {
		return readFromBytes(DataManager.getInstance().read(address), address);
	}
	
	Node getChild(int address) {
		Node child = getNode(address);
		child.setParentPos(pos);
		return child;
	}
	
	Node getRight() {
		Node right = getNode(this.rightPos);
		right.setParentPos(parentPos);
		return right;
	}

	private void initKeyAndValues() {
		keys = new Object[2*IMSetting.BN];
		values = new int[2*IMSetting.BN];
	}

	boolean isLeaf() {
		return isLeaf;
	}
	
	boolean isFull() {
		return n == 2*IMSetting.BN;
	}
	
	void add(Node node) throws IndexDuplicateException {
		add(node.keys[0], node.pos);
	}
	
	void add(final Object key, final int value) throws IndexDuplicateException {
		int i = n;
		while (i != 0 && IndexUtil.compareTo(keys[i-1], key, type) > 0) {
			keys[i] = keys[i-1];
			values[i] = values[i-1];
			i--;
		}
		//索引重复不被允许
		if (isLeaf() && i != 0 && IndexUtil.compareTo(key, keys[i-1], type) == 0) {
			throw new IndexDuplicateException();
		}
		keys[i] = key;
		values[i] = value;
		n++;
	}
	
	long remove(final Object key) {
		int i = 0;
		long value = -1;
		while (!key.equals(keys[i])) {
			i++;
		}
		value =values[i];
		while (i != n-1) {
			keys[i] = keys[i+1];
			values[i] = values[i+1];
			i++;
		}
		n--;
		return value;
	}
	
	void removeByIndex(int i) {
		while (i != n-1) {
			keys[i] = keys[i+1];
			values[i] = values[i+1];
			i++;
		}
		n--;
	}
	
	void updateKey(Object oldKey, Object newKey) {
		int i = 0;
		for (;!keys[i].equals(oldKey) && i < n; i++);
		
		if (i != n) {
			keys[i] = newKey; 
		}
	}
	
	void clear() {
		initKeyAndValues();
		n = 0;
	}
	
	void removeLast() {
		n--;
	}
	
	void setIsLeaf(boolean isLeaf) {
		this.isLeaf = isLeaf;
	}
	
	int addToDM() throws OutOfDiskSpaceException {
		pos = DataManager.getInstance().insert(writeToBytes(), TransactionManager.SUPER_ID);
		return pos;
	}
	
	void writeBackToDM() {
		DataManager.getInstance().update(pos, writeToBytes(), TransactionManager.SUPER_ID);
	}
	
	byte[] writeToBytes() {
		byte[] bytes = new byte[getBytesLen()];
		DataUtil.booleanToBytes(isLeaf(), 0, bytes);
		DataUtil.typeToBytes(type, 1, bytes);
		DataUtil.intToBytes(parentPos, 3, bytes);
		DataUtil.intToBytes(rightPos, 7, bytes);
		DataUtil.intToBytes(n, 11, bytes);
		writeKeyAndValueToBytes(bytes, 15);
		writeRightFirstkey(bytes, 15);
		return bytes;
	}
	
	private void writeRightFirstkey(byte[] bytes, int offset) {
		if (rightPos != -1) {
			switch(type){
	        case int32:
	        	DataUtil.intToBytes((Integer)rightFirstkey, offset + 2*IMSetting.BN*8, bytes);
	        	break;
	        case long64:
	        	DataUtil.longToBytes((Long)rightFirstkey, offset + 2*IMSetting.BN*12, bytes);
	        	break;
	        default://type == Type.string64
	        	DataUtil.stringToBytes((String)rightFirstkey, offset + 2*IMSetting.BN*136, bytes);
	        	break;
	        }
		} else {
			switch(type){
	        case int32:
	        	DataUtil.intToBytes(-1, offset + 2*IMSetting.BN*8, bytes);
	        	break;
	        case long64:
	        	DataUtil.longToBytes(-1, offset + 2*IMSetting.BN*12, bytes);
	        	break;
	        default://type == Type.string64
	        	DataUtil.stringToBytes(null, offset + 2*IMSetting.BN*136, bytes);
	        	break;
	        }
		}
	}
	
	private void writeKeyAndValueToBytes(byte[] bytes, int offset) {
		if (type == Type.int32) {
			int i = 0;
			for (; i < n; i++) {
				DataUtil.intToBytes((Integer)keys[i], offset + i*8, bytes);
				DataUtil.intToBytes(values[i], offset + i*8 + 4, bytes);
			}
			//补齐数组
			for (; i < 2*IMSetting.BN; i++) {
				DataUtil.intToBytes(-1, offset + i*8, bytes);
				DataUtil.intToBytes(-1, offset + i*8 + 4, bytes);
			}
		} else if (type == Type.long64) {
			int i = 0;
			for (; i < n; i++) {
				DataUtil.longToBytes((Long)keys[i], offset + i*12, bytes);
				DataUtil.intToBytes(values[i], offset + i*12 + 8, bytes);
			}
			//补齐数组
			for (; i < 2*IMSetting.BN; i++) {
				DataUtil.longToBytes(-1, offset + i*12, bytes);
				DataUtil.intToBytes(-1, offset + i*12 + 8, bytes);
			}
		} else { // newNode.type == Type.string64
			int i = 0;
			for (; i < n; i++) {
				DataUtil.stringToBytes((String)keys[i], offset + i*136, bytes);
				DataUtil.intToBytes(values[i], offset + i*136 + 132, bytes);
			}
			//补齐数组
			for (; i < 2*IMSetting.BN; i++) {
				DataUtil.stringToBytes(null, offset + i*136, bytes);
				DataUtil.intToBytes(-1, offset + i*136 + 132, bytes);
			}
		}
	}
	
	private int getBytesLen() {
		int len = 1+2+4+4+4;
		if (this.type == Type.int32) {
			len += 2*IMSetting.BN*(4+4);
		} else if (this.type == Type.long64) {
			len += 2*IMSetting.BN*(8+4);
		} else { // newNode.type == Type.string64
			len += 2*IMSetting.BN*(132+4);
		}
		len += type.getTypeLen();
		return len;
	}
	
	//从bytes读node
	static Node readFromBytes(byte[] bytes, int address) {
		//System.out.println("len" + bytes.length);
		Node newNode = new Node(DataUtil.bytesToBoolean(bytes, 0), DataUtil.bytesToType(1, bytes));
		newNode.pos = address;
		newNode.parentPos = DataUtil.bytesToInt(bytes, 3);
		newNode.rightPos = DataUtil.bytesToInt(bytes, 7);
		newNode.n = DataUtil.bytesToInt(bytes, 11);
		readKeyAndValueFromBytes(newNode, bytes, 15);
		if (newNode.rightPos != -1) {
			switch(newNode.type){
	        case int32:
	        	newNode.rightFirstkey = DataUtil.bytesToInt(bytes, 15 + 2*IMSetting.BN*8);
	        	break;
	        case long64:
	        	newNode.rightFirstkey = DataUtil.bytesToLong(bytes, 15 + 2*IMSetting.BN*12);
	        	break;
	        default://type == Type.string64
	        	newNode.rightFirstkey = DataUtil.bytesToString(bytes, 15 + 2*IMSetting.BN*136);
	        	break;
	        }
		}
		return newNode;
	}
	
	private static void readKeyAndValueFromBytes(Node newNode, byte[] bytes, int offset) {
		if (newNode.type == Type.int32) {
			for (int i = 0; i < newNode.n; i++) {
				newNode.keys[i] = new Integer(DataUtil.bytesToInt(bytes, offset + i*8));
				newNode.values[i] = DataUtil.bytesToInt(bytes, offset + i*8 + 4);
			}
		} else if (newNode.type == Type.long64) {
			for (int i = 0; i < newNode.n; i++) {
				newNode.keys[i] = new Long(DataUtil.bytesToLong(bytes, offset + i*12));
				newNode.values[i] = DataUtil.bytesToInt(bytes, offset + i*12 + 8);
			}
		} else { // newNode.type == Type.string64
			for (int i = 0; i < newNode.n; i++) {
				newNode.keys[i] = DataUtil.bytesToString(bytes, offset + i*136);
				newNode.values[i] = DataUtil.bytesToInt(bytes, offset + i*136 + 132);
			}
		}
	}
	
	
	
	
	@Override
	public String toString() {
		/*
		StringBuffer sb = new StringBuffer(pos + " " + isLeaf + " " + n + " ");
		for (int i = 0; i < n; i++) {
			sb.append("{" + keys[i] + "," + values[i] + "}");
		}
		sb.append('\n');
		return sb.toString();
		*/
		StringBuffer sb = new StringBuffer(isLeaf + "");
		for (int i = 0; i < n; i++) {
			sb.append(" " + keys[i]);
		}
		sb.append('\n');
		return sb.toString();
	}
	
	public String toTreeString() {
		StringBuffer sb = new StringBuffer(this.toString());
		if (!this.isLeaf()) {
			for (int i = 0; i < n; i++) {
				sb.append(Node.getNode(values[i]).toTreeString());
			}
		}
		return sb.toString();
	}
}
