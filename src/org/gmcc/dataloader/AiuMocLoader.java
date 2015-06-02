package org.gmcc.dataloader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.gmcc.utils.AiuMocRowkeyGenerator;
import org.gmcc.utils.HTableAdminFactoryImpl;
import org.gmcc.utils.TablePartionerManager;

public class AiuMocLoader{

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		//create table aiu_moc_
		int NUM_PARTION=4;
		int SALT_LEN=4;
		HTableAdminFactoryImpl hbaseAdmin=new HTableAdminFactoryImpl();

		hbaseAdmin.createTable("aiu_moc_20150219",TablePartionerManager.getStrPartionSplitKeys(NUM_PARTION,SALT_LEN), true);
		hbaseAdmin.close();
		
		AiuMocLoadThread amt=new AiuMocLoadThread(new File("/home/training/aiu-moc-cdr-201502191555-00001_20150219#20150219160201#_192.168.35.199.dat"),
				"aiu_moc_20150219",
				"/home/training/cdr_aiu_moc_conf.xml");
		amt.start();
		
		
		


	}

	
	
	

	
	

}
