package org.gmcc.utils.rowkey;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.gmcc.utils.StrUtils;

public class AiuMocRowkeyImpl extends RowkeyFactory {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		AiuMocRowkeyImpl rg=new AiuMocRowkeyImpl("/home/training/cdr_aiu_moc_conf.xml");
		//rg.addConfiguration();
		String cdr="";
		long s=System.currentTimeMillis();
		BufferedReader br=null;
		int counter=0;
		try {
			 br=new BufferedReader(new FileReader("/home/training/aiu-moc-cdr-201502191555-00001_20150219#20150219160201#_192.168.35.199.dat"));
		
			while((cdr=br.readLine())!=null)
			{
				//System.out.println(Bytes.toString(rg.genLongRowkey(cdr, 10)).length());
				System.out.println(Bytes.toString(rg.genLongRowkey(cdr, 4)));
				counter++;
			}
			//br.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
		System.out.println((System.currentTimeMillis()-s)+"ms\ncounter:"+counter);

	}
	int msisdnIdx=0;
	int starttimeIdx=0;
	int starttime_millisec_idx=0;
	int cdrIDIdx=0;
	String delim="";
	public AiuMocRowkeyImpl(String confpath)
	{
		this.addConfiguration(confpath);
	}
	public void addConfiguration(String confpath)
	{
		Configuration conf=new Configuration();
		conf.addResource(new Path(confpath));
		msisdnIdx=Integer.parseInt(conf.get("cdr.msisdn.idx"));
		starttimeIdx=Integer.parseInt(conf.get("cdr.starttime.idx"));
		starttime_millisec_idx=Integer.parseInt(conf.get("cdr.starttime.millisec.idx"));
		cdrIDIdx=Integer.parseInt(conf.get("cdr.cdrid.idx"));
		delim=conf.get("cdr.delim");
		String msisdn_len=conf.get("cdr.msisdn.len");
		if(msisdn_len!=null)
			this.MSISDN_LEN=Integer.parseInt(msisdn_len);
		String salt_len=conf.get("cdr.salt.len");
		if(salt_len!=null)
			this.SALT_LEN=Integer.parseInt(salt_len);
			
		//System.out.println(conf.get("msisdn.position"));
	}
	
	public byte[] genLongRowkey(String cdr,int numPartion)
	{
		String[] cdrs=StrUtils.split(cdr, delim);
		//System.out.println(cdr);
		if(cdrs.length!=0)
		{
			String cdrID=cdrs[cdrIDIdx];
			if(cdrID.length()>=4)
				cdrID=cdrID.substring(cdrID.length()-4);
			else
			{
				cdrID=StrUtils.leftPadWithZero((int)Math.random()*10000,4);//生成随机的四位以内数字作为cdrid
			}
			
			long st=Long.parseLong(cdrs[starttimeIdx])*1000+Long.parseLong(cdrs[starttime_millisec_idx]);
			if(!"".equals(cdrs[msisdnIdx]))
				return generateRowkey(numPartion, cdrs[msisdnIdx], st, cdrID);
		}
	
		return null;
	}

	public byte[] genShortRowkey(String cdr,int numPartion)
	{
		String[] cdrs=StrUtils.split(cdr, delim);
		if(cdrs.length!=0)
		{
			String cdrid=cdrs[cdrIDIdx];
			int cdrID=0;
			if(cdrid.length()>=4)
			{
				cdrid=cdrid.substring(cdrid.length()-4);
				cdrID=Integer.valueOf(cdrid,16);
				//System.out.println(cdrID);
			}
			else
			{
				cdrID=(int)Math.random()*10000;//生成随机的四位以内数字作为cdrid
				//cdrid=StrUtils.leftPadWithZero(cid, 4);
			}
			long msisdn=Long.parseLong(cdrs[msisdnIdx]);
			//long msisdn=cdrs[msisdnIdx].hashCode();
			long starttime=Long.parseLong(cdrs[starttimeIdx]);

			return generateRowkey(numPartion, msisdn, starttime, cdrID);
		}
		return null;
	}

}
