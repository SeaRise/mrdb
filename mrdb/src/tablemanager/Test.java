package tablemanager;

class Test {

	/**
	 * @param args
	 */
	static TableManager tbm = new TableManager();
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		/*
		tbm.createTable("t", new String[]{"a", "b", "c"}, new Type[] {Type.int32, Type.int32, Type.string64}, new boolean[] {true, true, false});
		for (int i = 0; i < 500; i++) {
			DataType[] dataTypes = new DataType[3];
			dataTypes[0] = new DataType(i, Type.int32);
			dataTypes[1] = new DataType(i/2, Type.int32);
			dataTypes[2] = new DataType(i + "", Type.string64);
			tbm.insert("t", dataTypes, false);
		}
		
		System.out.println("insert over");
		
		for (int i = 499; i >= 0; i--) {
			DataType[] keyTypes = new DataType[1];
			keyTypes[0] = new DataType(i/2, Type.int32);
			int[] keyIndexes = new int[1];
			keyIndexes[0] = 1;
		    tbm.read("t", keyTypes, keyIndexes);
		}
		
		System.out.println("---------------");
		
		for (int i = 499; i >= 0; i--) {
			DataType[] keyTypes = new DataType[2];
			keyTypes[0] = new DataType(i, Type.int32);
			keyTypes[1] = new DataType(i/2, Type.int32);
			int[] keyIndexes = new int[2];
			keyIndexes[0] = 0;
			keyIndexes[1] = 1;
		    tbm.read("t", keyTypes, keyIndexes);
		}
		
		DataType[] keyTypes = new DataType[2];
		keyTypes[0] = new DataType(500, Type.int32);
		keyTypes[1] = new DataType(500/2, Type.int32);
		int[] keyIndexes = new int[2];
		keyIndexes[0] = 0;
		keyIndexes[1] = 1;
	    tbm.read("t", keyTypes, keyIndexes);
		
		/*
		for (int i = 0; i < 500; i++) {
			DataType[] keyTypes = new DataType[1];
			keyTypes[0] = new DataType(i, Type.int32);
			Object[] newValues = new Object[1];
			newValues[0] = (500-i-1) + "";
			int[] indexes = {1};
			tbm.update("t", keyTypes, newValues, indexes);
		}
		
		for (int i = 499; i >= 0; i--) {
			DataType[] keyTypes = new DataType[1];
			keyTypes[0] = new DataType(i, Type.int32);
		    tbm.read("t", keyTypes);
		}*/
	}

}
