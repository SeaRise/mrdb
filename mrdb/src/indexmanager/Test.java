package indexmanager;

import datamanager.OutOfDiskSpaceException;
import tablemanager.Type;

class Test <E extends Comparable<E>> {
	
	static private IndexManager im = new IndexManager();
	
	public static void main(String[] args) throws OutOfDiskSpaceException, IndexDuplicateException {
		
		int rootAddress = im.addRootNode(Type.int32);
		
		for (int i = 9999; i > -1; i--) {
			im.insert(i, i, rootAddress, Type.int32);
			int ls = im.search(i, Type.int32, rootAddress);
			System.out.print(ls + "\n");
			//System.out.println(im.toString());
		}
		
		for (int i = 0; i < 10000; i++) {
			int ls = im.search(i, Type.int32, rootAddress);
		    System.out.print(ls + "\n");
		}
		System.out.println(im.toString());
	}
}
