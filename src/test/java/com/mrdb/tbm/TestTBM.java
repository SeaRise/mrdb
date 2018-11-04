package com.mrdb.tbm;

import java.io.IOException;

import org.junit.Test;

import com.mrdb.dm.exception.OutOfDiskSpaceException;
import com.mrdb.im.exception.IndexDuplicateException;
import com.mrdb.tbm.exception.NotSelectTableException;
import com.mrdb.tbm.exception.ObjectMismatchException;
import com.mrdb.tbm.exception.TableCreateFailExceotion;
import com.mrdb.tbm.exception.TableNameRepeatException;
import com.mrdb.tbm.exception.TableNotFoundException;
import com.mrdb.util.pool.BlockPoolExecutor;
import com.mrdb.util.pool.DataBlock;

public class TestTBM {
	
	static TableManager tbm = TableManager.getInstance();
	
	@Test
	public void test() throws TableNameRepeatException, IOException, OutOfDiskSpaceException, TableCreateFailExceotion, TableNotFoundException, NotSelectTableException, ObjectMismatchException, IndexDuplicateException  {
		tbm.createTable("fds", Type.int32);
		tbm.selectTable("fds");
		tbm.start();
		for (int i = 0; i < 2000; i++) {
			DataBlock db = BlockPoolExecutor.getInstance().getDataBlockMVCC(4);
			db.writeInt(0, i);
			tbm.insert(i, db, true);
			db.release();
		}
		tbm.commit();
		
		
		tbm.start();
		for (int i = 0; i < 2000; i++) {
			DataBlock db = tbm.read(i, true);
			//System.out.println(db==null);
			System.out.println(db.getInt(0));
			db.release();
		}
		tbm.commit();
		
		tbm.start();
		for (int i = 0; i < 2000; i++) {
			DataBlock db = BlockPoolExecutor.getInstance().getDataBlockMVCC(4);
			db.writeInt(0, -i);
			tbm.update(i, db, true);
			db.release();
		}
		tbm.commit();
		
		tbm.start();
		for (int i = 0; i < 2000; i++) {
			DataBlock db = tbm.read(i, true);
			//System.out.println(db==null);
			
			System.out.println(db.getInt(0));
			db.release();
		}
		tbm.commit();
	}
}
