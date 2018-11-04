package com.mrdb.dm.page;

import com.mrdb.dm.DMSetting;
import com.mrdb.util.DataUtil;
import com.mrdb.util.pool.BlockPoolExecutor;
import com.mrdb.util.pool.DataBlock;

public class Page {
	
	private byte[] pageContent;
	
	public Page() {
		this.pageContent = new byte[DMSetting.FRAME_SIZE];
	}
	
	public DataBlock getDataItem(int offset) {
		int len = DataUtil.bytesToInt(pageContent, offset);
		if (len == 0) {
			return null;
		}
		DataBlock block = BlockPoolExecutor.getInstance().getDataBlock(len);
		//前四位为dataItem.length
		byte[][] bytess = block.bytess;
		int pos = offset+4;
		int i = 0;
		while (i < bytess.length-1) {
			System.arraycopy(pageContent, pos, bytess[i], 0, bytess[i].length);
			i++;
			pos += bytess[i].length;
		}
		System.arraycopy(pageContent, pos, bytess[i], 0, len - (pos-offset-4));
		return block;
	}
	
	public void setDataItem(int offset, DataBlock dataItem) {
		if (dataItem == null) {
			DataUtil.intToBytes(0, offset, pageContent);
			return;
		}
		DataUtil.intToBytes(dataItem.length, offset, pageContent);
		//前四位为dataItem.length
		int destPos = offset+4;
		int i = 0;
		for (; i < dataItem.bytess.length-1; i++, destPos += dataItem.bytess[i].length) {
			System.arraycopy(dataItem.bytess[i], 0, pageContent, destPos, dataItem.bytess[i].length);
		}
		System.arraycopy(dataItem.bytess[i], 0, pageContent, destPos, dataItem.length - (destPos-offset-4));
	}
	
	byte[] getContent() {
		return pageContent;
	}
	
}
