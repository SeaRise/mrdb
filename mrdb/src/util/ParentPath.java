package util;

import java.io.File;

public class ParentPath {
	
	public static String rootPath = "G:"+File.separator+"ssdb"+File.separator;
	public static String dataFileParentPath = rootPath+"data"+File.separator;
	public static String tablesFileParentName = rootPath+"table"+File.separator;
	public static String tmFileParentName = rootPath+"transaction"+File.separator;
	
	public static void createPath() {
		new File(dataFileParentPath).mkdirs();
		new File(tablesFileParentName).mkdirs();
		new File(tmFileParentName).mkdirs();
	}
}
