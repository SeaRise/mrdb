package com.mrdb.util;

import com.mrdb.util.pool.DataBlock;

public class Entry {
	public int xmin;
	public int xmax;
	public DataBlock db;
	
	public Entry(DataBlock db, boolean read) {
		this.db = db;
		if (read) {
			xmin = db.getInt(db.length-8);
			xmax = db.getInt(db.length-4);
		} else {
			setXmax(0);
			setXmin(0);
		}
	}
	
	public void setXmax(int xmax) {
		this.xmax = xmax;
		db.writeInt(db.length-4, xmax);
	}
	
	public void setXmin(int xmin) {
		this.xmin = xmin;
		db.writeInt(db.length-8, xmin);
	}
	
	public void realse() {
		db.release();
	}
}
