package com.mrdb.im;

import org.junit.Test;

import com.mrdb.tbm.Type;

public class TestIM {

	static private IndexManager im = new IndexManager();
	
	@Test
	public void test() throws InterruptedException {
		int rootAddress = im.addRootNode(Type.int32);
		
		new Thread(new c1(rootAddress)).start();
		new Thread(new c2(rootAddress)).start();
		new Thread(new c3(rootAddress)).start();
		new Thread(new c4(rootAddress)).start();
		new Thread(new c5(rootAddress)).start();
		new Thread(new c6(rootAddress)).start();
		new Thread(new c7(rootAddress)).start();
		im.insert(1001, 1001, rootAddress, Type.int32);
		Thread.sleep(5000);
		System.out.println(im.toString());
	}
	
	static class c1 implements Runnable {
		int rootAddress;
		
		c1(int rootAddress) {
			this.rootAddress = rootAddress;
		}
		
		public void run() {
			for (int i = 0; i < 1000; i+=7) {
				System.out.println(i + " c1");
				im.insert(i, i, rootAddress, Type.int32);
				//System.out.println(im.toString());
			}
			for (int i = 0; i < 1000; i+=7) {
				System.out.println(i + " c1 " + im.search(i, Type.int32, rootAddress));
			}
			//System.out.println(im.toString());
		}
		
	}
	
	static class c2 implements Runnable {
		int rootAddress;
		c2(int rootAddress) {
			this.rootAddress = rootAddress;
		}
		
		public void run() {
			for (int i = 1; i < 1000; i+=7) {
				System.out.println(i + " c2");
				im.insert(i, i, rootAddress, Type.int32);
				//System.out.println(im.toString());
			}
			for (int i = 1; i < 1000; i+=7) {
				System.out.println(i + " c2 " + im.search(i, Type.int32, rootAddress));
			}
			//System.out.println(im.toString());
		}
	}
	
	static class c3 implements Runnable {
		int rootAddress;
		c3(int rootAddress) {
			this.rootAddress = rootAddress;
		}
		
		public void run() {
			for (int i = 2; i < 1000; i+=7) {
				System.out.println(i + " c3");
				im.insert(i, i, rootAddress, Type.int32);
				//System.out.println(im.toString());
			}
			for (int i = 2; i < 1000; i+=7) {
				System.out.println(i + " c3 " + im.search(i, Type.int32, rootAddress));
			}
			//System.out.println(im.toString());
		}
	}
	
	static class c4 implements Runnable {
		int rootAddress;
		c4(int rootAddress) {
			this.rootAddress = rootAddress;
		}
		
		public void run() {
			for (int i = 3; i < 1000; i+=7) {
				System.out.println(i + " c4");
				im.insert(i, i, rootAddress, Type.int32);
				//System.out.println(im.toString());
			}
			
			for (int i = 3; i < 1000; i+=7) {
				System.out.println(i + " c4 " + im.search(i, Type.int32, rootAddress));
			}
			//System.out.println(im.toString());
		}
	}
	
	static class c5 implements Runnable {
		int rootAddress;
		c5(int rootAddress) {
			this.rootAddress = rootAddress;
		}
		
		public void run() {
			for (int i = 4; i < 1000; i+=7) {
				System.out.println(i + " c5");
				im.insert(i, i, rootAddress, Type.int32);
				//System.out.println(im.toString());
			}
			for (int i = 4; i < 1000; i+=7) {
				System.out.println(i + " c5 " + im.search(i, Type.int32, rootAddress));
			}
			//System.out.println(im.toString());
		}
	}
	
	static class c6 implements Runnable {
		int rootAddress;
		c6(int rootAddress) {
			this.rootAddress = rootAddress;
		}
		
		public void run() {
			for (int i = 5; i < 1000; i+=7) {
				System.out.println(i + " c6");
				im.insert(i, i, rootAddress, Type.int32);
				//System.out.println(im.toString());
			}
			for (int i = 5; i < 1000; i+=7) {
				System.out.println(i + " c6 " + im.search(i, Type.int32, rootAddress));
			}
			//System.out.println(im.toString());
		}
	}
	
	static class c7 implements Runnable {
		int rootAddress;
		c7(int rootAddress) {
			this.rootAddress = rootAddress;
		}
		
		public void run() {
			for (int i = 6; i < 1000; i+=7) {
				System.out.println(i + " c7");
				im.insert(i, i, rootAddress, Type.int32);
				//System.out.println(im.toString());
			}
			for (int i = 6; i < 1000; i+=7) {
				System.out.println(i + " c7 " + im.search(i, Type.int32, rootAddress));
			}
			//System.out.println(im.toString());
		}
	}
}
