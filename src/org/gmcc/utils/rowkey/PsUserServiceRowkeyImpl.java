package org.gmcc.utils.rowkey;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.gmcc.utils.StrUtils;

public class PsUserServiceRowkeyImpl extends RowkeyFactory {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		PsUserServiceRowkeyImpl rg=new PsUserServiceRowkeyImpl("/home/training/cdr_ps_userservice_conf.xml");
		//rg.addConfiguration("/home/training/cdr_ps_userservice_conf.xml");
		String cdr="";
		long s=System.currentTimeMillis();
		BufferedReader br=null;
		int counter=0;
		try {
			 br=new BufferedReader(new FileReader("/home/training/user_service_CDR_201502191600_00016_20150219#20150219162002#_192.168.35.134.dat"));
		
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
//	int starttime_millisec_idx=0;
	int cdrIDIdx=0;
	String delim="";
	
	public PsUserServiceRowkeyImpl(String confpath)
	{
		this.addConfiguration(confpath);
	}
	@Override
	public void addConfiguration(String confpath) {
		// TODO Auto-generated method stub
		Configuration conf=new Configuration();
		conf.addResource(new Path(confpath));
		msisdnIdx=Integer.parseInt(conf.get("cdr.msisdn.idx"));
		starttimeIdx=Integer.parseInt(conf.get("cdr.starttime.idx"));
		//starttime_millisec_idx=Integer.parseInt(conf.get("cdr.starttime.millisec.idx"));
		cdrIDIdx=Integer.parseInt(conf.get("cdr.cdrid.idx"));
		delim=conf.get("cdr.delim");
		String msisdn_len=conf.get("cdr.msisdn.len");
		if(msisdn_len!=null)
			this.MSISDN_LEN=Integer.parseInt(msisdn_len);
		String salt_len=conf.get("cdr.salt.len");
		if(salt_len!=null)
			this.SALT_LEN=Integer.parseInt(salt_len);
		

	}

	@Override
	public byte[] genLongRowkey(String cdr, int numPartion) {
		// TODO Auto-generated method stub
		String[] cdrs=StrUtils.split(cdr, delim);
		//System.out.println(cdr);
		if(cdrs.length!=0)
		{
			String cdrID=cdrs[cdrIDIdx];
			//System.out.println("cdr id:"+cdrID);
			if(cdrID.length()>=4)
				cdrID=cdrID.substring(cdrID.length()-4);
			else
			{
				cdrID=StrUtils.leftPadWithZero((int)(Math.random()*10000),4);
				
			}
			
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
			Date d;
			Calendar c=Calendar.getInstance();
			try {
				d = format.parse(cdrs[starttimeIdx]);
				c.setTime(d);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return generateRowkey(numPartion, cdrs[msisdnIdx], c.getTimeInMillis(), cdrID);
		}
	
		return null;
	}

	@Override
	public byte[] genShortRowkey(String cdr, int numPartion) {
		// TODO Auto-generated method stub
		return null;
	}

}
