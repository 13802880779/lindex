package org.gmcc.dataloader;

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
import org.gmcc.utils.StrUtils;

public class RowkeyGenerator {

	/**
	 * 生成cdr的rowkey,rowkey设计原则:
	 * 1)salt(int)+msisdn反序(long)+starttime(cdr开始时间,long)+cdr_id后四位(int)
	 * 2)salt(string,8)+msisdn反序(String,12)+starttime(cdr开始时间,long)+cdr_id后四位(int)
	 */
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub		
	}
	
	int msisdnIdx=0;
	int starttimeIdx=0;
	int cdrIDIdx=0;
	String delim="";
	
	
	public void addConfiguration(String confpath)
	{
		Configuration conf=new Configuration();
		conf.addResource(new Path(confpath));
		msisdnIdx=Integer.parseInt(conf.get("cdr.msisdn.idx"));
		starttimeIdx=Integer.parseInt(conf.get("cdr.starttime.idx"));
		cdrIDIdx=Integer.parseInt(conf.get("cdr.cdrid.idx"));
		delim=conf.get("cdr.delim");
		//System.out.println(conf.get("msisdn.position"));
	}
	
	public byte[] genLongRowkey(String cdr,int numPartion)
	{
		String[] cdrs=StrUtils.split(cdr, delim);
		
		if(cdrs.length!=0)
		{
			String cdrID=cdrs[cdrIDIdx];
			if(cdrID.length()>=4)
				cdrID=cdrID.substring(cdrID.length()-4);
			else
			{
				cdrID=StrUtils.leftPadWithZero((int)Math.random()*10000,4);//生成随机的四位以内数字作为cdrid
			}
			return this.generateRowkey(numPartion, cdrs[msisdnIdx], cdrs[starttimeIdx], cdrID);
		}
	
		return null;
	}

	public byte[] genShortRowkey(String cdr,int numPartion)
	{
		String[] cdrs=StrUtils.split(cdr, delim);
		if(cdrs.length!=0)
		{
			long msisdn=Long.parseLong(cdrs[msisdnIdx]);
			long starttime=Long.parseLong(cdrs[starttimeIdx]);
			String cdrid=cdrs[cdrIDIdx];
			int cdrID=0;
			if(cdrid.length()>=4)
			{
				cdrid=cdrid.substring(cdrid.length()-4);
				cdrID=Integer.parseInt(cdrid);
			}
			else
			{
				cdrID=(int)Math.random()*10000;//生成随机的四位以内数字作为cdrid
				//cdrid=StrUtils.leftPadWithZero(cid, 4);
			}
			
			return this.generateRowkey(numPartion, msisdn, starttime, cdrID);
		}
		return null;
	}
	
	private byte[] generateRowkey(int numPartion, long msisdn,long starttime ,int cdrID)
	{
		byte[] rowkey=new byte[(Integer.SIZE+Long.SIZE+Long.SIZE+Integer.SIZE)/8];
		int salt=0;
		String revMsisdnStr=(new StringBuffer(""+msisdn)).reverse().toString();
		salt=revMsisdnStr.hashCode()%numPartion;		
		long revMsisdn=Long.parseLong(revMsisdnStr);
		int offset=0; 
		offset=Bytes.putInt(rowkey, offset, salt);
		offset=Bytes.putLong(rowkey, offset, revMsisdn);
		offset=Bytes.putLong(rowkey, offset, starttime);
		Bytes.putInt(rowkey, offset, cdrID);

		return rowkey;
	}
	
	private byte[] generateRowkey(int numPartion, String msisdn, String starttime, String cdrID)
	{
		String rowkey;
		int salt=0;
		String revMsisdnStr=(new StringBuffer(""+msisdn)).reverse().toString();
		salt=revMsisdnStr.hashCode()%numPartion;		
		
		rowkey=StrUtils.leftPadWithZero(salt,8);//salt补0到8位	
		rowkey+=StringUtils.leftPad(revMsisdnStr, 11,"0");//msisdn补0到11位
		rowkey+=StrUtils.long2datestr(Long.parseLong(starttime));//17位
		rowkey+=cdrID;//4位
		
		return Bytes.toBytes(rowkey);
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
