package tablemanager;

//作为带类型的数据的载体
public class DataType {
	
	Object value;
	Type type;
	
	public DataType(Object v, Type t) {
		value = v;
		type = t;
	}

	@Override
	public String toString() {
		return type + " " + value;
	}
	
	
}
