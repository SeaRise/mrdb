package transactionManager;

import tablemanager.DataType;
import tablemanager.Type;

class Test {

	/**
	 * @param args
	 */
	//static TableManager tbm = new TableManager();
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		/*
		tbm.createTable("t", new String[]{"a", "c"}, new Type[] {Type.int32, Type.string64}, new boolean[] {true, false});
			
		tbm.start();
		DataType[] dataTypes = new DataType[2];
		dataTypes[0] = new DataType(0, Type.int32);
		dataTypes[1] = new DataType(0 + "", Type.string64);
		tbm.insert("t", dataTypes, true);
		dataTypes[0] = new DataType(1, Type.int32);
		dataTypes[1] = new DataType(1 + "", Type.string64);
		tbm.insert("t", dataTypes, true);
		tbm.commit();
		
		tbm.start();
		dataTypes[0] = new DataType(2, Type.int32);
		dataTypes[1] = new DataType(2 + "", Type.string64);
		tbm.insert("t", dataTypes, true);
		dataTypes[0] = new DataType(3, Type.int32);
		dataTypes[1] = new DataType(3 + "", Type.string64);
		tbm.insert("t", dataTypes, true);
		tbm.abort();
		System.out.println("insert over");*/
		
		DataType[] keyTypes = new DataType[1];
		keyTypes[0] = new DataType(3, Type.int32);
		int[] keyIndexes = new int[1];
		keyIndexes[0] = 0;
		//tbm.read("t", keyTypes, keyIndexes);
	}

}
