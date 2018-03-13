package util;

import java.io.IOException;
import java.io.RandomAccessFile;

import tablemanager.Type;

public class DataUtil {
	
	 /**  
	    * byte数组中取boolean数值，1true,0false。和booleanToBytes（）配套使用 
	    */  
	public static boolean bytesToBoolean(byte[] src, int offset) {  
	    return (byte)(src[offset] & 0xFF) != (byte)0;  
	}  
	 /**  
	    * boolean数值写入byte数组中，1true,0false。和bytesToBoolean（）配套使用 
	    */  
	public static void booleanToBytes(boolean value, int offset, byte[] src) {
		src[offset] = value ? (byte)1 : (byte)0;
	}
	
	 /**  
	    * byte数组中取int数值，本方法适用于(低位在后，高位在前)的顺序。和intToBytes（）配套使用 
	    */  
	public static int bytesToInt(byte[] src, int offset) {  
	    int value;    
	    value = (int) ( ((src[offset] & 0xFF)<<24)  
	            |((src[offset+1] & 0xFF)<<16)  
	            |((src[offset+2] & 0xFF)<<8)  
	            |(src[offset+3] & 0xFF));  
	    return value;  
	}  
	/**  
	    * int数值写入byte数组中，本方法适用于(低位在后，高位在前)的顺序。和bytesToInt（）配套使用 
	    */  
	public static void intToBytes(int value, int offset, byte[] src) {
		src[offset] = (byte) ((value>>24) & 0XFF);
		src[offset+1] = (byte) ((value>>16) & 0XFF);
		src[offset+2] = (byte) ((value>>8) & 0XFF);
		src[offset+3] = (byte) (value & 0XFF);
	}
	/**
	 * byte数组中取char数值，本方法适用于(低位在后，高位在前)的顺序。和charToBytes（）配套使用 
	 */
	public static char bytesToChar(byte[] src, int offset) {  
	    char value = (char) ( ((src[offset] & 0xFF)<<8)  
	            |(src[offset+1] & 0xFF));  
	    return value;  
	}  
	/**  
	    * char数值写入byte数组中，本方法适用于(低位在后，高位在前)的顺序。和bytesToChar（）配套使用 
	    */  
	public static void charToBytes(char c, int offset, byte[] src) {
		src[offset] = (byte) ((c>>8) & 0XFF);
		src[offset+1] = (byte) (c & 0XFF);
	}
	 /**  
	    * byte数组中取long数值，本方法适用于(低位在后，高位在前)的顺序。和LongToBytes（）配套使用 
	    */  
	public static int bytesToLong(byte[] src, int offset) {  
	    int value;    
	    value = (int) ( ((src[offset] & 0xFF)<<56)  
	            |((src[offset+1] & 0xFF)<<48)  
	            |((src[offset+2] & 0xFF)<<40)  
	            |((src[offset+3] & 0xFF)<<32)  
	            |((src[offset+4] & 0xFF)<<24)  
	            |((src[offset+5] & 0xFF)<<16)  
	            |((src[offset+6] & 0xFF)<<8)  
	            |(src[offset+7] & 0xFF));  
	    return value;  
	}  
	/**  
	    * long数值写入byte数组中，本方法适用于(低位在后，高位在前)的顺序。和bytesToLong（）配套使用 
	    */  
	public static void longToBytes(long value, int offset, byte[] src) {
		src[offset] = (byte) ((value>>56) & 0XFF);
		src[offset+1] = (byte) ((value>>48) & 0XFF);
		src[offset+2] = (byte) ((value>>40) & 0XFF);
		src[offset+3] = (byte) ((value>>32) & 0XFF);
		src[offset+4] = (byte) ((value>>24) & 0XFF);
		src[offset+5] = (byte) ((value>>16) & 0XFF);
		src[offset+6] = (byte) ((value>>8) & 0XFF);
		src[offset+7] = (byte) (value & 0XFF);
	}
	
	//byte转为字符串
	public static String bytesToString(byte[] src, int offset) {
		int len = bytesToInt(src, offset);
		char[] chars = new char[len];
		for (int i = 0; i < len; i++) {
			chars[i] = bytesToChar(src, offset+4+i*2);
		}
		return new String(chars);
	}
	
	//132bit的字符串写入byte数组
	public static void stringToBytes(String value, int offset, byte[] src) {
		char[] chars = value.toCharArray();
		if (chars.length > 64) {
			//throws exception错误体系尚未建立
			System.out.println("over 64");
		}
		
		intToBytes(chars.length, offset, src);
		int i = 0;
		//4为长度占用的字节数
		for (; i < chars.length; i++) {
			charToBytes(chars[i], offset+4+i*2, src);
		}
		while (i < 64) { //补齐64个字符
			charToBytes((char) 0, offset+4+i*2, src);
			i++;
		}
	}
	
	public static Type bytesToType(int offset, byte[] src) {
		if (src[offset] == (byte)1) {
			return Type.string64;
		} else { // src[offset] == (byte)0
			if (src[offset+1] == (byte)0) {
				return Type.int32;
			} else { // src[offset+1] == (byte)1
				return Type.long64;
			}
		}
	}
	
	public static void typeToBytes(Type type, int offset, byte[] src) {
		if (type.equals(Type.int32)) {
			src[offset] = (byte)0;
			src[offset+1] = (byte)0;
		} else if (type.equals(Type.long64)) {
			src[offset] = (byte)0;
			src[offset+1] = (byte)1;
		} else {//Type.string64
			src[offset] = (byte)1;
			src[offset+1] = (byte)0;
		}
	}
	
	
	//定长的string读写,固定为132字节
	public static void writeStringBlockIntoFile(RandomAccessFile file, String s) throws IOException {
		char[] chars = s.toCharArray();
		if (chars.length > 64) {
			//throws exception错误体系尚未建立
			System.out.println("over 64");
		}
		
		file.writeInt(chars.length);
		int i = 0;
		//4为长度占用的字节数
		for (; i < chars.length; i++) {
			file.writeChar(chars[i]);
		}
		while (i < 64) { //补齐64个字符
			file.writeChar((char) 0);
			i++;
		}
	}
	
	public static long[] intersect(long[] arr1, long[] arr2) {   
		long[] togetherArray = new long[arr1.length < arr2.length ? 
										arr1.length : arr2.length];
		int start = 0;
	    for (int i = 0; i < arr1.length; i++) {
	        for (int j = 0; j < arr2.length; j++) {
	            if (arr1[i] == arr2[j]) {
	            	if (arr1[i] == -1) {
	            		break;
	            	}
	                togetherArray[start++] = arr1[i];
	            }
	        }
	    }
	    if (start != togetherArray.length) {
	    	togetherArray[start] = -1;
	    }
	    
	    return togetherArray;
    }   
	
	public static String readStringBlockFromFile(RandomAccessFile file) throws IOException {
		int len = file.readInt();
		char[] chars = new char[len];
		for (int i = 0; i < len; i++) {
			chars[i] = file.readChar();
		}
		return new String(chars);
	}
	
	//不定长的String读写
	public static void writeStringIntoFile(RandomAccessFile file, String s) throws IOException {
		byte[] tn = s.getBytes();
		file.writeInt(tn.length);
		file.write(tn);
	}
	
	public static String readStringFromFile(RandomAccessFile file) throws IOException {
		int len = file.readInt();
		byte[] fn = new byte[len];
		file.readFully(fn);
		return new String(fn);
	}
	
}
