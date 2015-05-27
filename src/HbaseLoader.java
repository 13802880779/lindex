import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.StringTokenizer;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;


public class HbaseLoader {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws ParseException 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		scanIndex();
	}
	public static void scanIndex() throws Exception
	{
		int bk=50;
		String startdatastr="2015-02-02 01:00:00";
		String enddatastr="2015-02-02 23:59:00";
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long startt=(df.parse(startdatastr)).getTime();
		long endt=(df.parse(enddatastr)).getTime();
		//System.out.println(new Date(startt).toString());
	
		int longlength=Long.SIZE/8;
		int intlength=Integer.SIZE/8;
		byte[] startkey=new byte[intlength+intlength+longlength+intlength+intlength+longlength];//salt(int)+bankuai(int)+timestamp(long)+tradedetail rowkey
		byte[] endkey=new byte[intlength+intlength+longlength+intlength+intlength+longlength];//salt(int)+bankuai(int)+timestamp(long)+tradedetail rowkey

		int numregionserver=2;
		HTablePool pool=new HTablePool();
		HTableInterface ht=pool.getTable("bkindex");
		//System.out.println("==>"+endt);

		ArrayList<Get> getlist=new ArrayList<Get>();
		
		
		for(int i=0; i<numregionserver; i++)
		{
			int offset=0;
			offset=Bytes.putInt(startkey, offset, i);
			offset=Bytes.putInt(startkey, offset, bk);
			offset=Bytes.putLong(startkey, offset, startt);
			offset=Bytes.putInt(startkey, offset, 0);
			offset=Bytes.putInt(startkey, offset, 0);
			offset=Bytes.putLong(startkey, offset, 0L);
			//System.out.println(offset);
			offset=0;			
			offset=Bytes.putInt(endkey, offset, i);
			offset=Bytes.putInt(endkey, offset, bk);
			offset=Bytes.putLong(endkey, offset, endt);
			offset=Bytes.putInt(endkey, offset, 0);
			offset=Bytes.putInt(endkey, offset, 0);
			offset=Bytes.putLong(endkey, offset, 0L);

			
			
			Scan s=new Scan();
			s.setCaching(1000);
			s.setStartRow(startkey);
			s.setStopRow(endkey);
			///System.out.println(i);
			
			
			ResultScanner rs=ht.getScanner(s);
			int counter=0;
			for(Result r:rs)
			{
				counter++;
				byte[] rowkey=Arrays.copyOfRange(r.getRow(), 16, 32);
				Get g=new Get(rowkey);
				g.addColumn(Bytes.toBytes("CF"), Bytes.toBytes("C"));
				getlist.add(g);
			}
			System.out.println("==>"+counter);
		}
		
		
		ht=pool.getTable("tradedetail");
		Result r[]=ht.get(getlist);
		for(int i=0;i<r.length;i++)
		{
			System.out.println(Bytes.toString(r[i].getValue(Bytes.toBytes("CF"), Bytes.toBytes("C"))));
		}
		
		ht.close();
		
		
	}
	public static void buildIndex() throws Exception
	{
		System.out.println("hbase build index started:"+(new Date()).toGMTString());
		long s=System.currentTimeMillis();
		
		BufferedReader br=new BufferedReader(new FileReader("/home/training/2015-02_update.csv"));
		HTablePool pool=new HTablePool();
		HTableInterface ht=pool.getTable("bkindex");
		ArrayList<Put> putlist=new ArrayList<Put>();
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String[] tokens=new String[9];
		String row;
		int counter=0;
		int tokenindex=0;
		int longlength=Long.SIZE/8;
		int intlength=Integer.SIZE/8;
		byte[] rowkey=new byte[intlength+intlength+longlength+intlength+intlength+longlength];//salt(int)+bankuai(int)+timestamp(long)+tradedetail rowkey
		int numregionserver=2;
		int batchsize=10000;
		
		while((row=br.readLine())!=null)
		{
			counter++;
			
			StringTokenizer st=new StringTokenizer(row,",");
			tokenindex=0;
			while(st.hasMoreTokens())
			{
				tokens[tokenindex]=st.nextToken();
				tokenindex++;
			}
			int stock=Integer.parseInt(tokens[0]);
			String datestr=tokens[1]+" "+tokens[2];
			Date d=df.parse(datestr);
			long ts=d.getTime();
			int salt=tokens[0].hashCode()%numregionserver;
			int bk=Integer.parseInt(tokens[8]);
			int offset=0;
			offset=Bytes.putInt(rowkey, offset, salt);	
			offset=Bytes.putInt(rowkey, offset, bk);	
			offset=Bytes.putLong(rowkey, offset, ts);	
			//System.out.println(Bytes.toBytes(salt));
			//offset=Bytes.putBytes(rowkey, offset, Bytes.toBytes(salt), 0, intlength);
			//System.out.println(rowkey);
			//offset=Bytes.putBytes(rowkey, offset, Bytes.toBytes(stock), 0, intlength);

			//Bytes.putBytes(rowkey, offset, Bytes.toBytes(ts), 0, longlength);
			
			
			offset=Bytes.putInt(rowkey, offset, salt);			
			offset=Bytes.putInt(rowkey, offset, stock);			
			Bytes.putLong(rowkey, offset, ts);
			
			Put p=new Put(rowkey);
			p.add(Bytes.toBytes("CF"), Bytes.toBytes("C"), Bytes.toBytes(""));
			
			putlist.add(p);
			if(putlist.size()>batchsize)
			{
				ht.put(putlist);//batch insert
				ht.flushCommits();
				//counter=0;//counter return 0;
				putlist.clear();
			}
			
		}
		if(putlist.size()>0)
		{
			ht.put(putlist);
			ht.flushCommits();
		}
		
		ht.close();
		br.close();
		System.out.println("hbase build index ended:"+(new Date()).toGMTString()+",used time:"+(System.currentTimeMillis()-s)+" ms,inserted "+counter+" rows");

	}
	
	
	public static void loadfile() throws Exception
	{
		System.out.println("hbaseload started:"+(new Date()).toGMTString());
		long s=System.currentTimeMillis();
		
		BufferedReader br=new BufferedReader(new FileReader("/home/training/2015-02_update.csv"));
		HTablePool pool=new HTablePool();
		HTableInterface ht=pool.getTable("tradedetail");
		ArrayList<Put> putlist=new ArrayList<Put>();
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String[] tokens=new String[9];
		String row;
		int counter=0;
		int tokenindex=0;
		int longlength=Long.SIZE/8;
		int intlength=Integer.SIZE/8;
		byte[] rowkey=new byte[intlength+intlength+longlength];//salt(int)+stock(int)+timestamp(long)
		int numregionserver=2;
		int batchsize=10000;
		
		while((row=br.readLine())!=null)
		{
			counter++;
			
			StringTokenizer st=new StringTokenizer(row,",");
			tokenindex=0;
			while(st.hasMoreTokens())
			{
				tokens[tokenindex]=st.nextToken();
				tokenindex++;
			}
			int stock=Integer.parseInt(tokens[0]);
			String datestr=tokens[1]+" "+tokens[2];
			Date d=df.parse(datestr);
			long ts=d.getTime();
			int salt=tokens[0].hashCode()%numregionserver;
			int offset=0;
			//System.out.println(Bytes.toBytes(salt));
			//offset=Bytes.putBytes(rowkey, offset, Bytes.toBytes(salt), 0, intlength);
			//System.out.println(rowkey);
			//offset=Bytes.putBytes(rowkey, offset, Bytes.toBytes(stock), 0, intlength);

			//Bytes.putBytes(rowkey, offset, Bytes.toBytes(ts), 0, longlength);
			
			
			offset=Bytes.putInt(rowkey, offset, salt);			
			offset=Bytes.putInt(rowkey, offset, stock);			
			Bytes.putLong(rowkey, offset, ts);
			
			Put p=new Put(rowkey);
			p.add(Bytes.toBytes("CF"), Bytes.toBytes("C"), Bytes.toBytes(row));
			
			putlist.add(p);
			if(putlist.size()>batchsize)
			{
				ht.put(putlist);//batch insert
				ht.flushCommits();
				//counter=0;//counter return 0;
				putlist.clear();
			}
			
		}
		if(putlist.size()>0)
		{
			ht.put(putlist);
			ht.flushCommits();
		}
		
		ht.close();
		br.close();
		System.out.println("hbaseload ended:"+(new Date()).toGMTString()+",used time:"+(System.currentTimeMillis()-s)+" ms,inserted "+counter+" rows");
	
	}

}