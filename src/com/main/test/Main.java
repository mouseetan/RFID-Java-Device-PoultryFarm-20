package com.main.test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Observer;
import java.util.Random;

import com.module.interaction.RXTXListener;
import com.module.interaction.ReaderHelper;
import com.rfid.RFIDReaderHelper;
import com.rfid.ReaderConnector;
import com.rfid.rxobserver.RXObserver;
import com.rfid.rxobserver.ReaderSetting;
import com.rfid.rxobserver.bean.RXInventoryTag;
import com.util.StringTool;

import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class Main {
	static ReaderHelper rHelper;
	
	// 参数设置
	static String device_no = "1";		// 设备编号
	static byte curWorkingAntenna = 0;	// 当前工作天线

	// 用于标签查重的散列表
	static Map<String, RXInventoryTag> tagMap = new Map<String, RXInventoryTag>();

	static Observer mObserver = new RXObserver() {

		/*AspectCode-1-Start*/
		@Override
		protected void onInventoryTag(RXInventoryTag tag) {
			// 在一次盘存周期中，只统计第一次出现时的标签
			if( tagMap.get(tag.strEPC) == null ){
				tagMap.put(tag.strEPC, tag);
			}
		}
		/*AspectCode-1-End*/
		
		/*AspectCode-2-Start*/
		@Override
		protected void onInventoryTagEnd(RXInventoryTag.RXInventoryTagEnd endTag) {
			// 输出此次盘存到的标签总数
			System.out.println("tag count:" + endTag.mTotalRead);
			// 将通过散列表去重后的标签上传
			for(RXInventoryTag tag: tagMap.values()){
				uploadData(tag);
			}
			tagMap.clear(); // 上传完毕后，将此次盘存的标签清空
			waitFor(2000);	// 等待2秒后开始下一次盘存
			((RFIDReaderHelper) rHelper).realTimeInventory((byte) 0xff,(byte)0x01);
		}
		/*AspectCode-2-End*/
		

		/*AspectCode-3-Start*/
		@Override
		protected void onExeCMDStatus(byte cmd,byte status) {
			((RFIDReaderHelper) rHelper).realTimeInventory((byte) 0xff,(byte)0x01);
			System.out.format("CMD:%s  Execute status:%S \n",
					String.format("%02X",cmd),String.format("%02x", status));
		}
		/*AspectCode-3-End*/
		
	};
	
	private static void waitFor(int milisecond) {
		try {
			Thread.sleep(milisecond);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	private static void uploadData(RXInventoryTag tag) {
		// tag.strEPC EPC编号; tag.strFreq 频率; tag.strRSSI 信号强度; tag.btAntId 天线编号; tag.strPC
		OkHttpClient client = new OkHttpClient().newBuilder().build();
		MediaType mediaType = MediaType.parse("application/x-www-form-urlencoded");
		StringBuffer urlEncodedFormData = new StringBuffer();
		urlEncodedFormData.append("epc=" + tag.strEPC + "&");
		urlEncodedFormData.append("rfid_reader_id=" + device_no + "&");
		urlEncodedFormData.append("rfid_reader_antenna_id=" + tag.btAntId + "&");
		urlEncodedFormData.append("rss=" + tag.strRSSI + "&");
		urlEncodedFormData.append("frequency=" + tag.strFreq);
		RequestBody body = RequestBody.create(mediaType, urlEncodedFormData.toString());
		Request request = new Request.Builder()
			.url("http://129.204.171.195:8080/addPoultryRecord")
			.method("POST", body)
			.addHeader("Content-Type", "application/x-www-form-urlencoded")
			.build();
		try {
			Response response = client.newCall(request).execute();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static void main(String[] args) {
		
		final ReaderConnector rConnector = new ReaderConnector();	// 创建连接器
		rHelper = rConnector.connectNet("192.168.50.3", 4001);		// 连接RFID阅读器
		if(rHelper != null) {
			System.out.println("Connect success!");
			try {
				rHelper.registerObserver(mObserver); // 设置观察者，当有事件发生时，执行观察者对应事件的回调方法
				((RFIDReaderHelper) rHelper).setWorkAntenna((byte) 0xFF, (byte) 0x03);	// 设置工作天线
				((RFIDReaderHelper) rHelper).realTimeInventory((byte) 0xFF,(byte)0x01);	// 开始实时盘存
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			System.out.println("Connect faild!");
			rConnector.disConnect();
		} 
	}
}
