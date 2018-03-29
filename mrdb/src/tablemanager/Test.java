package tablemanager;

import indexmanager.IndexDuplicateException;

import java.io.IOException;

import util.pool.BlockPoolExecutor;
import util.pool.DataBlock;
import datamanager.OutOfDiskSpaceException;

class Test {

	/**
	 * @param args
	 */
	static TableManager tbm = TableManager.getInstance();
	
	public static void main(String[] args) throws TableNameRepeatException, IOException, OutOfDiskSpaceException, TableCreateFailExceotion, TableNotFoundException, NotSelectTableException, ObjectMismatchException, IndexDuplicateException {
		tbm.createTable("fds", Type.int32);
		tbm.selectTable("fds");
		tbm.start();
		for (int i = 0; i < 5; i++) {
			DataBlock db = BlockPoolExecutor.getInstance().getDataBlockMVCC(4);
			db.writeInt(0, i);
			tbm.insert(i, db, true);
			db.release();
		}
		tbm.commit();
		
		tbm.start();
		for (int i = 0; i < 5; i++) {
			DataBlock db = tbm.read(i, true);
			//System.out.println(db==null);
			System.out.println(db.getInt(0));
			db.release();
		}
		tbm.commit();
	}

}
