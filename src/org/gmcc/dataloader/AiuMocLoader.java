package org.gmcc.dataloader;

import java.io.BufferedReader;
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

public class AiuMocLoader {

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
		HColumnDescriptor hcd=hbaseAdmin.getColumnDescriptor("AD", -1, 1, null, null);
		ArrayList<HColumnDescriptor> l=new ArrayList();		
		l.add(hcd);
		hbaseAdmin.createTable("aiu_moc_20150219", l, TablePartionerManager.getStrPartionSplitKeys(NUM_PARTION,SALT_LEN), true);
		hbaseAdmin.close();
		
		HTablePool htp=new HTablePool();
		HTableInterface htable=htp.getTable("aiu_moc_20150219");
		List<Put> putList=new ArrayList();
		//int counter=0;
		
		AiuMocRowkeyGenerator rg=new AiuMocRowkeyGenerator();
		rg.addConfiguration("/home/training/cdr_aiu_moc_conf.xml");
		String cdr="";
		long s=System.currentTimeMillis();
		BufferedReader br=null;
		//int counter=0;
		try {
			 br=new BufferedReader(new FileReader("/home/training/aiu-moc-cdr-201502191555-00001_20150219#20150219160201#_192.168.35.199.dat"));
		
			while((cdr=br.readLine())!=null)
			{
				//System.out.println(Bytes.toString(rg.genLongRowkey(cdr, 10)).length());
				
				
				Put p=new Put(rg.genShortRowkey(cdr, NUM_PARTION));
				p.add(Bytes.toBytes("AD"), Bytes.toBytes("Q"), Bytes.toBytes(cdr));
				putList.add(p);
				//counter++;
				if(putList.size()>1000)
				{
					htable.put(putList);
					htable.flushCommits();
					putList.clear();
				}
			}
			
			if(putList.size()>0)
			{
				htable.put(putList);
				htable.flushCommits();
				putList.clear();
			}
			
		
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				br.close();
				htable.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
		System.out.println((System.currentTimeMillis()-s)+"ms");

	}

}
