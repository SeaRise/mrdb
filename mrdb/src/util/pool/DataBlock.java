package util.pool;

import java.io.IOException;
import java.io.RandomAccessFile;

import tablemanager.Type;
import util.DataUtil;

//内容后移和string读写还没搞定
public class DataBlock {
	
	public final byte[][] bytess;
	
	private final BlockPoolExecutor executor;
	
	private final boolean shouldRelease;
	
	public int length;
	
	DataBlock(byte[][] bytess, int len, BlockPoolExecutor executor, boolean shouldRelease) {
		this.shouldRelease = shouldRelease;
		this.bytess = bytess;
		this.executor = executor;
		this.length = len;
	}
	
	public void movesDown(int offset, int downShift, int len) {
		int endi = getI(offset);
		int endipos = getIpos(endi, offset);
		int j = getI(offset+len-1);
		int jpos = getIpos(j, offset+len-1);
		while(j > endi || (j == endi && jpos >= endipos)) {
			int ipos = jpos + downShift;
			int num = ipos / BlockPoolExecutor.BYTES_SIZE;
			ipos -= num*BlockPoolExecutor.BYTES_SIZE;
			int i = j + num;
			bytess[i][ipos] = bytess[j][jpos];
			
			if (jpos == 0) {
				jpos = BlockPoolExecutor.BYTES_SIZE-1;
				j--;
			} else {
				jpos--;
			}
		}
	}
	
	public void writeToFile(RandomAccessFile file) throws IOException {
		int i = 0;
		for (; i < bytess.length-1; i++) {
			file.write(bytess[i]);
		}
		file.write(bytess[i], 0, length - i*BlockPoolExecutor.BYTES_SIZE);
	}
	
	static public DataBlock readFromFile(RandomAccessFile file, int len) throws IOException {
		if (len == 0) {
			return null;
		}
		DataBlock block = BlockPoolExecutor.getInstance().getDataBlock(len);
		int i = 0;
		for (; i < block.bytess.length-1; i++) {
			file.readFully(block.bytess[i]);
		}
		file.readFully(block.bytess[i], 0, block.length - i*BlockPoolExecutor.BYTES_SIZE);
		return block;
	}
	
	private int getI(int pos) {
		return pos / BlockPoolExecutor.BYTES_SIZE;
	}
	
	private int getIpos(int i, int pos) {
		return pos - i*BlockPoolExecutor.BYTES_SIZE;
	}
	
	public void writeInt(int pos, int value) {
		int i = getI(pos);
		int ipos = getIpos(i, pos);
		if (ipos <= BlockPoolExecutor.BYTES_SIZE-4) {
			DataUtil.intToBytes(value, ipos, bytess[i]);
		} else {
			//分写在两个数组
			int shift = 24;
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
	
	public int getInt(int pos) {
    	int i = getI(pos);
		int ipos = getIpos(i, pos);
		if (ipos <= BlockPoolExecutor.BYTES_SIZE-4) {
			return DataUtil.bytesToInt(bytess[i], ipos);
		} else {
			//分读在两个数组
			int value = 0;    
			int shift = 24;
			while (shift >= 0) {
				if (ipos == bytess[i].length) {
					i++;
					ipos = 0;
				}
				value |= (long) (bytess[i][ipos] & 0xFF) << shift;
				ipos++;
				shift -= 8;
			}
			return value;
		}
	}
    
	public void writeLong(int pos, long value) {
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
	
	public long getLong(int pos) {
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
				value |= (long) (bytess[i][ipos] & 0xFF) << shift;
				ipos++;
				shift -= 8;
			}
			return value;
		}
	}
    
	public void writeBoolean(int pos, boolean value) {
    	DataUtil.booleanToBytes(value, pos, bytess[getI(pos)]);
    }
    
	public boolean getBoolean(int pos) {
    	return DataUtil.bytesToBoolean(bytess[getI(pos)], pos);
    }
    
	public void writeType(int pos, Type value) {
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
    
	public Type getType(int pos) {
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
    
	
    //先留着,暂时不写
    //一定会分写在多个数组
	public void writeString(int pos, String value) {
    	
	}
    //一定会分读在多个数组
	public String getString(int pos) {
		return null;
	}
	
	//调用该方法后不可再用此对象
	public void release() {
		if (shouldRelease) {
			for (int i = 0; i < bytess.length && executor.addBytes(bytess[i]); i++);
			length = 0;
		}
	}
}
