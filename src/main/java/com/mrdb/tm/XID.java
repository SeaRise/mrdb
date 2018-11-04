package com.mrdb.tm;


public enum XID {
	active((byte)0),
	commit((byte)1),
	aborted((byte)2);
	
	private final byte b;
	
	XID(byte b) {
		this.b = b;
	}
	
	byte getByte() {
		return b;
	}
	
	static XID getXID(byte b) {
		switch(b){
			case (byte)0 : return active;
			case (byte)1 : return commit;
			case (byte)2 : return aborted;
			default : return null;
		}
	}
}
