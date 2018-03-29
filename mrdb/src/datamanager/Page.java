package datamanager;

import util.DataUtil;
import util.pool.BlockPoolExecutor;
import util.pool.DataBlock;

class Page {
	
	private byte[] pageContent;
	
	Page() {
		this.pageContent = new byte[DMSetting.FRAME_SIZE];
	}
	
	DataBlock getDataItem(int offset) {
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
	
	void setDataItem(int offset, DataBlock dataItem) {
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
