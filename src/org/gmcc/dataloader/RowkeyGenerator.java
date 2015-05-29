package org.gmcc.dataloader;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class RowkeyGenerator {

	/**
	 * 生成cdr的rowkey,rowkey设计原则:
	 * 1)salt(int)+msisdn反序(long)+starttime(cdr开始时间,long)+cdr_id后四位(int)
	 * 2)salt(int)+msisdn反序(String)+starttime(cdr开始时间,long)+cdr_id后四位(int)
	 */
	static RowkeyGenerator rg=new RowkeyGenerator();
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		RowkeyGenerator rg=RowkeyGenerator.create();
		rg.addConfgurationPath("/home/training/myconf.xml");
		
	}
	
	public static RowkeyGenerator create()
	{
		return rg;
	}
	
	public byte[] generateRowkey(int salt, long msisdn,long starttime ,int cdrID)
	{
		Long msisdnL=new Long(msisdn);
		//char s[]=msisdnL.to
		return null;
	}
	
	public static void addConfgurationPath(String fpath)
	{
		Configuration conf=new Configuration();
		conf.addResource(new Path(fpath));
		//System.out.println(conf.get("msisdn.position"));
		Collection<String> list=conf.getStringCollection("msisdn.position");
		Iterator<String> it=list.iterator();
		//System.out.println("==>"+list.size());
		while(it.hasNext())
		{
			String s=it.next();
			System.out.println("==>"+s);
		}
	}

}
