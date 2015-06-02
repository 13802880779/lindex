package org.gmcc.utils;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;

public abstract  class RowkeyFactory {

	/**
	 * 生成cdr的rowkey,rowkey设计原则:
	 * 1)salt(int)+msisdn反序(long)+starttime(cdr开始时间,long)+cdr_id后四位(int)
	 * 2)salt(string,8)+msisdn反序(String,12)+starttime(cdr开始时间,long)+cdr_id后四位(int)
	 */
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub	
	}
	

	protected abstract void addConfiguration(String path);
	protected abstract byte[] genLongRowkey(String cdr,int numPartion);
	protected abstract byte[] genShortRowkey(String cdr,int numPartion);

	public int MSISDN_LEN=11;
	public int SALT_LEN=8; 

	
	protected byte[] generateRowkey(int numPartion, long msisdn,long starttime ,int cdrID)
	{
	//	System.out.println("short rowkey generator:\nnumPartinon:"+numPartion+"\nmsisdn:"+msisdn+"\nstarttime"+starttime+"\ncdrID:"+cdrID);
		byte[] rowkey=new byte[(Integer.SIZE+Long.SIZE+Long.SIZE+Integer.SIZE)/8];
		int salt=0;
		String revMsisdnStr=(new StringBuffer(""+msisdn)).reverse().toString();
	//	System.out.println(revMsisdnStr);
		salt=Math.abs(revMsisdnStr.hashCode())%numPartion;		
		long revMsisdn=Long.parseLong(revMsisdnStr);
		int offset=0; 
		offset=Bytes.putInt(rowkey, offset, salt);
		offset=Bytes.putLong(rowkey, offset, revMsisdn);
		offset=Bytes.putLong(rowkey, offset, starttime);
		Bytes.putInt(rowkey, offset, cdrID);

		return rowkey;
	}
	
	protected byte[] generateRowkey(int numPartion, String msisdn, String starttime, String cdrID)
	{
	//	System.out.println("long rowkey generator:\nnumPartinon:"+numPartion+"\nmsisdn:"+msisdn+"\nstarttime:"+starttime+"\ncdrID:"+cdrID);

		return this.generateRowkey(numPartion, msisdn, Long.parseLong(starttime), cdrID);	
	
	}
	
	protected byte[] generateRowkey(int numPartion, String msisdn, long starttime, String cdrID)
	{
	//	System.out.println("long rowkey generator:\nnumPartinon:"+numPartion+"\nmsisdn:"+msisdn+"\nstarttime:"+starttime+"\ncdrID:"+cdrID);

		StringBuffer rowkey=new StringBuffer();
		int salt=0;
		if(msisdn.length()>this.MSISDN_LEN)
			msisdn=msisdn.substring(0,this.MSISDN_LEN);
		
		String revMsisdnStr=(new StringBuffer(""+msisdn)).reverse().toString();
		salt=Math.abs(revMsisdnStr.hashCode())%numPartion;		
		//System.out.println("hashcode:"+revMsisdnStr.hashCode());

		rowkey.append(StrUtils.leftPadWithZero(salt,SALT_LEN)+"|");//salt补0到8位	
		rowkey.append(StringUtils.leftPad(revMsisdnStr, MSISDN_LEN,"0")+"|");//msisdn补0到11位
		rowkey.append(StrUtils.long2datestr(starttime)+"|");//17位
		rowkey.append(cdrID);//4位
		
		//System.out.println("rowkey before bytes:"+rowkey);
		return Bytes.toBytes(rowkey.toString());
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
