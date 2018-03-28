import indexmanager.IndexDuplicateException;

import java.io.IOException;

import tablemanager.NotSelectTableException;
import tablemanager.ObjectMismatchException;
import tablemanager.TableCreateFailExceotion;
import tablemanager.TableManager;
import tablemanager.TableNameRepeatException;
import tablemanager.TableNotFoundException;
import tablemanager.Type;
import util.pool.BlockPoolExecutor;
import util.pool.DataBlock;
import datamanager.OutOfDiskSpaceException;



public class Test {
	
	static TableManager tbm = TableManager.getInstance();
	
	public static void main(String[] args) throws TableNameRepeatException, IOException, OutOfDiskSpaceException, TableCreateFailExceotion, TableNotFoundException, NotSelectTableException, ObjectMismatchException, IndexDuplicateException {
		tbm.createTable("fds", Type.int32);
		tbm.selectTable("fds");
		tbm.start();
		for (int i = 0; i < 5; i++) {
			DataBlock db = BlockPoolExecutor.getInstance().getDataBlock(4);
			db.writeInt(0, i);
			tbm.insert(i, db, false);
			db.release();
		}
		tbm.commit();
		for (int i = 0; i < 5; i++) {
			DataBlock db = tbm.read(i);
			System.out.println(db.getInt(0));
			db.release();
		}
	}
}
