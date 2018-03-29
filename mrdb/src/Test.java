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
	}
	
	static class c1 implements Runnable {
		@Override
		public void run() {
			try {
				tbm.selectTable("fdss");
				tbm.start();
				for (int i = 199; i >= 1; i-=3) {
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
				for (int i = 199; i >= 1; i-=3) {
					DataBlock db = tbm.read(i, true);
					System.out.print("c1 " + i + " ");
					System.out.println(db.getInt(0));
					db.release();
				}
				tbm.commit();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (TableNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NotSelectTableException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ObjectMismatchException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (OutOfDiskSpaceException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IndexDuplicateException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}
	
	static class c2 implements Runnable {
		@Override
		public void run() {
			try {
				tbm.selectTable("fdss");
				tbm.start();
				for (int i = 198; i >= 0; i-=3) {
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
				for (int i = 198; i >= 0; i-=3) {
					DataBlock db = tbm.read(i, true);
					System.out.print("c2 " + i + " ");
					System.out.println(db.getInt(0));
					db.release();
				}
				tbm.commit();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (TableNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NotSelectTableException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ObjectMismatchException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (OutOfDiskSpaceException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IndexDuplicateException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	static class c3 implements Runnable {
		@Override
		public void run() {
			try {
				tbm.selectTable("fdss");
				tbm.start();
				for (int i = 2; i < 200; i+=3) {
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
				for (int i = 2; i < 200; i+=3) {
					DataBlock db = tbm.read(i, true);
					System.out.print("c3 " + i + " ");
					System.out.println(db.getInt(0));
					db.release();
				}
				tbm.commit();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (TableNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NotSelectTableException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ObjectMismatchException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (OutOfDiskSpaceException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IndexDuplicateException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
