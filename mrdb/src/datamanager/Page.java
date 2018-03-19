package datamanager;

import java.util.Arrays;

import util.DataUtil;

class Page {
	
	private byte[] pageContent;
	
	Page() {
		this.pageContent = new byte[DMSetting.FRAME_SIZE];
	}
	
	byte[] getDataItem(int offset) {
		int len = DataUtil.bytesToInt(pageContent, offset);
		//前四位为dataItem.length
		return Arrays.copyOfRange(pageContent, offset+4, offset+4+len);
	}
	
	void setDataItem(int offset, byte[] dataItem) {
		DataUtil.intToBytes(dataItem.length, offset, pageContent);
		//前四位为dataItem.length
		for (int i = 0; i < dataItem.length; i++) {
			pageContent[offset+4+i] = dataItem[i];
		}
	}
	
	byte[] getContent() {
		return pageContent;
	}
	
}
