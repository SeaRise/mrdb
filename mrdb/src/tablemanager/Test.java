package tablemanager;

import java.io.IOException;

import datamanager.OutOfDiskSpaceException;

class Test {

	/**
	 * @param args
	 */
	static TableManager tbm = new TableManager();
	
	public static void main(String[] args) throws TableNameRepeatException, IOException, OutOfDiskSpaceException, TableCreateFailExceotion {
		tbm.createTable("fds", Type.int32);
	}

}
