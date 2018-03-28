package util.pool;

import java.io.IOException;
import java.io.RandomAccessFile;

public class Test {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		RandomAccessFile dbFile = new RandomAccessFile("G:\\mrdb\\data\\d.txt", "rw");
		DataBlock b = BlockPoolExecutor.getInstance().getDataBlock(4);
		b.writeInt(0, 5);
		System.out.println(b.getInt(0));
		b.writeToFile(dbFile);
		dbFile.seek(0);
		System.out.println(DataBlock.readFromFile(dbFile, 4).getInt(0));
	}

}
