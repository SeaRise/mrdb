package com.mrdb;

import java.io.IOException;

import org.junit.Test;

import com.mrdb.dm.exception.OutOfDiskSpaceException;
import com.mrdb.im.exception.IndexDuplicateException;
import com.mrdb.tbm.TableManager;
import com.mrdb.tbm.Type;
import com.mrdb.tbm.exception.NotSelectTableException;
import com.mrdb.tbm.exception.ObjectMismatchException;
import com.mrdb.tbm.exception.TableCreateFailExceotion;
import com.mrdb.tbm.exception.TableNameRepeatException;
import com.mrdb.tbm.exception.TableNotFoundException;
import com.mrdb.util.pool.BlockPoolExecutor;
import com.mrdb.util.pool.DataBlock;

public class TestMrdb {
static TableManager tbm = TableManager.getInstance();
	
	@Test
	public void test() throws TableNameRepeatException, IOException, OutOfDiskSpaceException, TableCreateFailExceotion, TableNotFoundException, NotSelectTableException, ObjectMismatchException, IndexDuplicateException {
		tbm.createTable("fdss", Type.int32);
		//tbm.selectTable("fds");
		/*
		tbm.start();
		for (int i = 0; i < 5; i++) {
			DataBlock db = BlockPoolExecutor.getInstance().getDataBlockMVCC(4);
			db.writeInt(0, i);
			tbm.insert(i, db, true);
			db.release();
		}
		tbm.commit();
		for (int i = 0; i < 5; i++) {
			DataBlock db = tbm.read(i);
			System.out.println(db.getInt(0));
			db.release();
		}*/
		new Thread(new c1()).start();
		new Thread(new c2()).start();
		new Thread(new c3()).start();
		new Thread(new c4()).start();
		new Thread(new c5()).start();
		new Thread(new c6()).start();
		new Thread(new c7()).start();
	}
	
	static class c1 implements Runnable {
		public void run() {
			tbm.selectTable("fdss");
			tbm.start();
			for (int i = 0; i < 500; i+=7) {
				DataBlock db = BlockPoolExecutor.getInstance().getDataBlockMVCC(4);
				db.writeInt(0, i);
				tbm.insert(i, db, true);
				db.release();
				/*
				db = tbm.read(i, true);
				System.out.print("c1 " + i + " ");
				System.out.println(db.getInt(0));
				db.release();*/
			}
			tbm.commit();
			
			tbm.start();
			for (int i = 0; i < 500; i+=7) {
				DataBlock db = tbm.read(i, true);
				System.out.print("c1 " + i + " ");
				System.out.println(db.getInt(0));
				db.release();
			}
			tbm.commit();
		}
		
	}
	
	static class c2 implements Runnable {
		public void run() {
			tbm.selectTable("fdss");
			tbm.start();
			for (int i = 1; i < 500; i+=7) {
				DataBlock db = BlockPoolExecutor.getInstance().getDataBlockMVCC(4);
				db.writeInt(0, i);
				tbm.insert(i, db, true);
				db.release();
				/*
				db = tbm.read(i, true);
				System.out.print("c2 " + i + " ");
				System.out.println(db.getInt(0));
				db.release();*/
			}
			tbm.commit();
			
			tbm.start();
			for (int i = 1; i < 500; i+=7) {
				DataBlock db = tbm.read(i, true);
				System.out.print("c2 " + i + " ");
				System.out.println(db.getInt(0));
				db.release();
			}
			tbm.commit();
		}
	}
	
	static class c3 implements Runnable {
		public void run() {
			tbm.selectTable("fdss");
			tbm.start();
			for (int i = 2; i < 500; i+=7) {
				DataBlock db = BlockPoolExecutor.getInstance().getDataBlockMVCC(4);
				db.writeInt(0, i);
				tbm.insert(i, db, true);
				db.release();
				/*
				db = tbm.read(i, true);
				System.out.print("c3 " + i + " ");
				System.out.println(db.getInt(0));
				db.release();*/
			}
			tbm.commit();
			
			tbm.start();
			for (int i = 2; i < 500; i+=7) {
				DataBlock db = tbm.read(i, true);
				System.out.print("c3 " + i + " ");
				System.out.println(db.getInt(0));
				db.release();
			}
			tbm.commit(); 
		}
	}
	
	static class c4 implements Runnable {
		public void run() {
			tbm.selectTable("fdss");
			tbm.start();
			for (int i = 3; i < 500; i+=7) {
				DataBlock db = BlockPoolExecutor.getInstance().getDataBlockMVCC(4);
				db.writeInt(0, i);
				tbm.insert(i, db, true);
				db.release();
				/*
				db = tbm.read(i, true);
				System.out.print("c3 " + i + " ");
				System.out.println(db.getInt(0));
				db.release();*/
			}
			tbm.commit();
			
			tbm.start();
			for (int i = 3; i < 500; i+=7) {
				DataBlock db = tbm.read(i, true);
				System.out.print("c4 " + i + " ");
				System.out.println(db.getInt(0));
				db.release();
			}
			tbm.commit(); 
		}
	}
	
	static class c5 implements Runnable {
		public void run() {
			tbm.selectTable("fdss");
			tbm.start();
			for (int i = 4; i < 500; i+=7) {
				DataBlock db = BlockPoolExecutor.getInstance().getDataBlockMVCC(4);
				db.writeInt(0, i);
				tbm.insert(i, db, true);
				db.release();
				/*
				db = tbm.read(i, true);
				System.out.print("c3 " + i + " ");
				System.out.println(db.getInt(0));
				db.release();*/
			}
			tbm.commit();
			
			tbm.start();
			for (int i = 4; i < 500; i+=7) {
				DataBlock db = tbm.read(i, true);
				System.out.print("c5 " + i + " ");
				System.out.println(db.getInt(0));
				db.release();
			}
			tbm.commit(); 
		}
	}
	
	static class c6 implements Runnable {
		public void run() {
			tbm.selectTable("fdss");
			tbm.start();
			for (int i = 5; i < 500; i+=7) {
				DataBlock db = BlockPoolExecutor.getInstance().getDataBlockMVCC(4);
				db.writeInt(0, i);
				tbm.insert(i, db, true);
				db.release();
				/*
				db = tbm.read(i, true);
				System.out.print("c3 " + i + " ");
				System.out.println(db.getInt(0));
				db.release();*/
			}
			tbm.commit();
			
			tbm.start();
			for (int i = 5; i < 500; i+=7) {
				DataBlock db = tbm.read(i, true);
				System.out.print("c5 " + i + " ");
				System.out.println(db.getInt(0));
				db.release();
			}
			tbm.commit(); 
		}
	}
	
	static class c7 implements Runnable {
		public void run() {
			tbm.selectTable("fdss");
			tbm.start();
			for (int i = 6; i < 500; i+=7) {
				DataBlock db = BlockPoolExecutor.getInstance().getDataBlockMVCC(4);
				db.writeInt(0, i);
				tbm.insert(i, db, true);
				db.release();
				/*
				db = tbm.read(i, true);
				System.out.print("c3 " + i + " ");
				System.out.println(db.getInt(0));
				db.release();*/
			}
			tbm.commit();
			
			tbm.start();
			for (int i = 6; i < 500; i+=7) {
				DataBlock db = tbm.read(i, true);
				System.out.print("c5 " + i + " ");
				System.out.println(db.getInt(0));
				db.release();
			}
			tbm.commit(); 
		}
	}
}
