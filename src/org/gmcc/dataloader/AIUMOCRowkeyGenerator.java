package org.gmcc.dataloader;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.gmcc.utils.StrUtils;

public class AIUMOCRowkeyGenerator extends RowkeyGenerator {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		AIUMOCRowkeyGenerator rg=new AIUMOCRowkeyGenerator();
		rg.addConfiguration("/home/training/cdr_cs_moc_conf.xml");
		String cdr="1424332921,1424332242,183,0,0,3,12600,12199,0,1,0,0,,,460003005439325,863654024728610,00560F3A,137sc+JOkEWvP4=9441,0,137ktWsTd1wRUE=4092,460,00,A5F2,9309,,24,1172,,,1182,2268,3028,3030,6564,7388,8056,,,,,13920,13935,13935,10,1,8,2,11,1,05690350710f5314,0,,05690350710f5314,0,13809229441,A5F2,9309,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,05690350710f5314,0";
		System.out.println(Bytes.toString(rg.genLongRowkey(cdr, 10)).length());
		System.out.println(StringUtils.leftPad("731", 10,"0"));

	}
	int msisdnIdx=0;
	int starttimeIdx=0;
	int starttime_millisec_idx=0;
	int cdrIDIdx=0;
	String delim="";
	
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
			
			return generateRowkey(numPartion, cdrs[msisdnIdx], st, cdrID);
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
			
			return generateRowkey(numPartion, msisdn, starttime, cdrID);
		}
		return null;
	}

}
