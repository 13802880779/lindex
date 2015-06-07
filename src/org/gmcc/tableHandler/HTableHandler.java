package org.gmcc.tableHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.gmcc.utils.StrUtils;

public class HTableHandler {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		HTableHandler hth =new HTableHandler("ps_userservice_20150219");
		String msisdn="86139vG3OLQ7pXFU=3129";
		String starttime="20150219000000000";
		String endtime="20150219235959999";
		try {
			
			ArrayList al=hth.scan(msisdn,starttime,endtime);
			for(Object cdr:al)
			{
				System.out.println((String)cdr);
			}
			//188BDnY0HQISUo=7720,139vG3OLQ7pXFU=3129
			/*ArrayList al=hth.getAllCalledMsisdns(msisdn, starttime, endtime);
			for(Object called:al)
			{
				ArrayList al2=hth.getAllCalledMsisdns((String)called, starttime, endtime);
				for(Object called2:al2)
				{
					hth.getAllCalledMsisdns((String)called2, starttime, endtime);
				}
			}*/
			//hth.scanRelations(msisdn, starttime, endtime,5,0,new HashMap());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally
		{
			try {
				hth.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	HTablePool tablePool;
	HTableInterface htable;
	public HTableHandler(String tableName)
	{
		tablePool=new HTablePool();
		htable=tablePool.getTable(tableName);
	}
	public ArrayList getAllCalledMsisdns(String msisdn,String starttime,String endtime) throws IOException
	{
		//ArrayList rs= scan("137GKzusyrGYb4=9883", "20150219000000000", "20150219235959999");
		ArrayList rs= scan(msisdn, starttime, endtime);
		ArrayList called=new ArrayList();
		Iterator itt=rs.iterator();
		HashMap hs=new HashMap();
		while(itt.hasNext())
		{
			String cdr=(String)itt.next();
			//System.out.println(cdr);
			String[] cdrItem=StrUtils.split(cdr, ",");
			String calledMsisdn=cdrItem[17];
			hs.put(calledMsisdn, 1);
		}
		
		Set set = hs.keySet();
		Iterator iter = set.iterator();
		while (iter.hasNext()) {
		String key = (String) iter.next();
		System.out.println(msisdn+"==>"+key);
		called.add(key);
		}
		return called;
	}
	
	public void query(String rowkey)
	{
		
	}
	
	public ArrayList scan(String msisdn,String starttime,String endtime) throws IOException
	{
		Scan scan=new Scan();
		StringBuffer rowkey=new StringBuffer();
		int salt=0;
		ArrayList result=new ArrayList();
	
		String revMsisdnStr=(new StringBuffer(""+msisdn)).reverse().toString();
		salt=Math.abs(revMsisdnStr.hashCode())%4;	//numPartion	

		rowkey.append(StrUtils.leftPadWithZero(salt,4)+"|");
		rowkey.append(StringUtils.leftPad(revMsisdnStr, 21,"0")+"|");//msisdn补0到11位,应该从conf中读取
		
		String startkey=rowkey.toString()+starttime;
		String endkey=rowkey.toString()+endtime;
		
		scan.setStartRow(Bytes.toBytes(startkey));
		scan.setStopRow(Bytes.toBytes(endkey));
		scan.setCaching(1000);
		ResultScanner rs=htable.getScanner(scan);
		
		for(Result r:rs)
		{
			String cdr=Bytes.toString(r.getValue(Bytes.toBytes("cf"), Bytes.toBytes("q")));
			result.add(cdr);
		}
		return result;
		
		
	}
	public void scanRelations(String msisdn,String starttime,String endtime,int stoplevel,int currlevel,HashMap msisdnlist) throws IOException
	{
	//	System.out.println("currlevel "+currlevel);
		if((currlevel>stoplevel)||msisdnlist.containsKey(msisdn))
			return;
		msisdnlist.put(msisdn, currlevel);
		Scan scan=new Scan();
		StringBuffer rowkey=new StringBuffer();
		int salt=0;
		ArrayList result=new ArrayList();
	
		String revMsisdnStr=(new StringBuffer(""+msisdn)).reverse().toString();
		salt=Math.abs(revMsisdnStr.hashCode())%4;	//numPartion	

		rowkey.append(StrUtils.leftPadWithZero(salt,4)+"|");
		rowkey.append(StringUtils.leftPad(revMsisdnStr, 19,"0")+"|");//msisdn补0到11位
		
		String startkey=rowkey.toString()+starttime;
		String endkey=rowkey.toString()+endtime;
		
		scan.setStartRow(Bytes.toBytes(startkey));
		scan.setStopRow(Bytes.toBytes(endkey));
		scan.setCaching(1000);
		ResultScanner rs=htable.getScanner(scan);
		
		for(Result r:rs)
		{
			String cdr=Bytes.toString(r.getValue(Bytes.toBytes("cf"), Bytes.toBytes("q")));
			//result.add(cdr);
			String[] cdrItem=StrUtils.split(cdr, ",");
			String calledMsisdn=cdrItem[17];
			if(!msisdnlist.containsKey(calledMsisdn))
			{
				//msisdnlist.put(calledMsisdn, currlevel);
				scanRelations(calledMsisdn,starttime,endtime,stoplevel,currlevel+1,msisdnlist);
				System.out.println("level "+(currlevel)+": "+calledMsisdn);
			}
			
			
			
		}
		
		
		
	}
	
	public void close() throws IOException
	{
		htable.close();
	}

}
