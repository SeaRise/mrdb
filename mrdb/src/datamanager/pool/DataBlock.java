package datamanager.pool;

import tablemanager.Type;
import util.DataUtil;

public class DataBlock {
	
	private final byte[][] bytess;
	
	private final BlockPoolExecutor executor;
	
	DataBlock(byte[][] bytess, BlockPoolExecutor executor) {
		this.bytess = bytess;
		this.executor = executor;
	}
	
	private int getI(int pos) {
		return pos / BlockPoolExecutor.BYTES_SIZE;
	}
	
	private int getIpos(int i, int pos) {
		return pos - i*BlockPoolExecutor.BYTES_SIZE;
	}
	
	void writeInt(int pos, int value) {
		int i = getI(pos);
		int ipos = getIpos(i, pos);
		if (ipos <= BlockPoolExecutor.BYTES_SIZE-4) {
			DataUtil.intToBytes(value, ipos, bytess[i]);
		} else {
			//分写在两个数组
		}
	}
	
    int getInt(int pos) {
    	int i = getI(pos);
		int ipos = getIpos(i, pos);
		if (ipos <= BlockPoolExecutor.BYTES_SIZE-4) {
			return DataUtil.bytesToInt(bytess[i], ipos);
		} else {
			//分读在两个数组
		}
		return -1;
	}
    
    void writeLong(int pos, long value) {
    	int i = getI(pos);
		int ipos = getIpos(i, pos);
		if (ipos <= BlockPoolExecutor.BYTES_SIZE-4) {
			DataUtil.longToBytes(value, ipos, bytess[i]);
		} else {
			//分写在两个数组
			int shift = 56;
			while (shift >= 0) {
				if (ipos == bytess[i].length) {
					i++;
					ipos = 0;
				}
				bytess[i][ipos] = (byte) ((value>>shift) & 0XFF);;
				ipos++;
				shift -= 8;
			}
		}
	}
	
    long getLong(int pos) {
    	int i = getI(pos);
		int ipos = getIpos(i, pos);
		if (ipos <= BlockPoolExecutor.BYTES_SIZE-8) {
			return DataUtil.bytesToLong(bytess[i], ipos);
		} else {
			//分读在两个数组
			long value = 0;    
			int shift = 56;
			while (shift >= 0) {
				if (ipos == bytess[i].length) {
					i++;
					ipos = 0;
				}
				value |= (long) (bytess[i][ipos] & 0xFF) << 56;
				ipos++;
				shift -= 8;
			}
			return value;
		}
	}
    
    void writeBoolean(int pos, boolean value) {
    	DataUtil.booleanToBytes(value, pos, bytess[getI(pos)]);
    }
    
    boolean readBoolean(int pos) {
    	return DataUtil.bytesToBoolean(bytess[getI(pos)], pos);
    }
    
    void writeType(int pos, Type value) {
    	int i = getI(pos);
		int ipos = getIpos(i, pos);
		if (ipos <= BlockPoolExecutor.BYTES_SIZE-2) {
			DataUtil.typeToBytes(value, ipos, bytess[i]);
		} else {
			//分写在两个数组
			if (value.equals(Type.int32)) {
				bytess[i][ipos] = (byte)0;
				bytess[i+1][0] = (byte)0;
			} else if (value.equals(Type.long64)) {
				bytess[i][ipos] = (byte)0;
				bytess[i+1][0] = (byte)1;
			} else {//Type.string64
				bytess[i][ipos] = (byte)1;
				bytess[i+1][0] = (byte)0;
			}
		}
		
    }
    
    Type getType(int pos) {
    	int i = getI(pos);
		int ipos = getIpos(i, pos);
		if (ipos <= BlockPoolExecutor.BYTES_SIZE-2) {
			return DataUtil.bytesToType(ipos, bytess[i]);
		} else {
			//分读在两个数组
			if (bytess[i][ipos] == (byte)1) {
				return Type.string64;
			} else { // src[offset] == (byte)0
				if (bytess[i+1][0] == (byte)0) {
					return Type.int32;
				} else { // src[offset+1] == (byte)1
					return Type.long64;
				}
			}
		}
    }
    
    //先留着,暂时不谢
    //一定会分写在多个数组
    void writeString(int pos, String value) {
    	
	}
    //一定会分读在多个数组
    String getString(int pos) {
		return null;
	}
	
	//调用该方法后不可再用此对象
	public void release() {
		for (int i = 0; i < bytess.length && executor.addBytes(bytess[i]); i++);
	}
}
