package com.mrdb.im;

import com.mrdb.tbm.Type;

class IndexUtil {

	static int compareTo(Object o1, Object o2, Type type) {
		if (type == Type.int32) {
			//return (((Integer) o1).intValue() - ((Integer) o2).intValue());
			return ((Integer) o1).compareTo((Integer) o2);
		} else if (type == Type.long64) {
			//return (((Long) o1).intValue() - ((Long) o2).intValue());
			return ((Long) o1).compareTo((Long) o2);
		} else { //type == Type.string64
			return ((String) o1).compareTo((String) o2);
		}
	}
	
}
