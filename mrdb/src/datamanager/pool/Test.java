package datamanager.pool;

public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		byte[][] bytess = new byte[5][];
		for (int i = 0; i < bytess.length; i++) {
			bytess[i] = new byte[32];
		}
		
		DataBlock b = new DataBlock(bytess, null);
		b.writeInt(31, 5);
		System.out.println(b.getInt(31));
	}

}
