package indexmanager;

import datamanager.OutOfDiskSpaceException;
import tablemanager.Type;

class Test <E extends Comparable<E>> {
	
	static private IndexManager im = new IndexManager();
	
	public static void main(String[] args) throws OutOfDiskSpaceException, IndexDuplicateException, InterruptedException {
		
		int rootAddress = im.addRootNode(Type.int32);
		//System.out.println(rootAddress);
		
		/*
		for (int i = 0; i < 6000; i++) {
			im.insert(i, i, rootAddress, Type.int32);
			int ls = im.search(i, Type.int32, rootAddress);
			System.out.print(ls + "\n");
			//System.out.println(im.toString());
		}
		
		
		for (int i = 0; i < 6000; i++) {
			int ls = im.search(i, Type.int32, rootAddress);
		    System.out.print(ls + "\n");
		}
		System.out.println(im.toString());*/
		
		new Thread(new c1(rootAddress)).start();
		new Thread(new c2(rootAddress)).start();
		new Thread(new c3(rootAddress)).start();
	}
	
	static class c1 implements Runnable {
		int rootAddress;
		
		c1(int rootAddress) {
			this.rootAddress = rootAddress;
		}
		
		@Override
		public void run() {
			for (int i = 199; i >= 1; i-=3) {
				try {
					System.out.println(i + " c1");
					im.insert(i, i, rootAddress, Type.int32);
					//System.out.println(im.toString());
				} catch (OutOfDiskSpaceException | IndexDuplicateException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				//int ls = im.search(i, Type.int32, rootAddress);
				//System.out.print(ls + "\n");
				//System.out.println(im.toString());
			}
			System.out.println(im.toString());
		}
		
	}
	
	static class c2 implements Runnable {
		int rootAddress;
		c2(int rootAddress) {
			this.rootAddress = rootAddress;
		}
		
		@Override
		public void run() {
			for (int i = 198; i >= 0; i-=3) {
				try {
					System.out.println(i + " c2");
					im.insert(i, i, rootAddress, Type.int32);
					//System.out.println(im.toString());
				} catch (OutOfDiskSpaceException | IndexDuplicateException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				//int ls = im.search(i, Type.int32, rootAddress);
				//System.out.print(ls + "\n");
				//System.out.println(im.toString());
			}
			System.out.println(im.toString());
		}
	}
	
	static class c3 implements Runnable {
		int rootAddress;
		c3(int rootAddress) {
			this.rootAddress = rootAddress;
		}
		
		@Override
		public void run() {
			for (int i = 2; i < 200; i+=3) {
				try {
					System.out.println(i + " c3");
					im.insert(i, i, rootAddress, Type.int32);
					//System.out.println(im.toString());
				} catch (OutOfDiskSpaceException | IndexDuplicateException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				//int ls = im.search(i, Type.int32, rootAddress);
				//System.out.print(ls + "\n");
				//System.out.println(im.toString());
			}
			System.out.println(im.toString());
		}
	}
}
