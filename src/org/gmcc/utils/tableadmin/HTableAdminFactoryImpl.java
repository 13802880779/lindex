package org.gmcc.utils.tableadmin;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;


public class HTableAdminFactoryImpl implements HTableAdminFactory {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		//HTableAdminFactoryImpl hfi=new HTableAdminFactoryImpl();
		/*HColumnDescriptor hcd=hfi.getColumnDescriptor("all_data", 3000, 1, null, null);
		HColumnDescriptor hcd2=hfi.getColumnDescriptor("all_data2", 3000, 3, null, null);
		ArrayList<HColumnDescriptor> l=new ArrayList();
		l.add(hcd);
		l.add(hcd2);
		hfi.createTable("test3", l, TablePartionerManager.getPartionSplitKeys(4), true);*/
	//	hfi.createTable("test3","all_data",true);
		//hfi.close();
		String s="2";
		System.out.println(Bytes.toBytes(s).toString());
	}
	
	HBaseAdmin hbaseAdmin=null;
	//HTablePool hp=null;
	int TIME_TO_LIVE=-1;
	int MAX_VERSION=1;
	BloomType BLOOM_TYPE=BloomType.ROW;
	Compression.Algorithm COMPRESSION_ALG=Compression.Algorithm.SNAPPY;
	
	public HTableAdminFactoryImpl() throws Exception
	{
		Configuration conf=HBaseConfiguration.create();

	//	conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());  
	//	conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		hbaseAdmin=new HBaseAdmin(conf);
	//	hp=new HTablePool();
		
		
	}
	public void close() throws IOException
	{
		if(null!=hbaseAdmin)
			hbaseAdmin.close();
		//if(null!=hp)
		//	hp.close();
	}
	/*public HTableInterface getTable(String tableName) {
		// TODO Auto-generated method stub
		if((null==tableName)||tableName.equals(""))	
			return null;
			
		HTableInterface ht=hp.getTable(tableName);
		
		return ht;
		
	}*/
	
	public HBaseAdmin getHbaseAdmin()
	{
		return this.hbaseAdmin;
	}

	@Override
	public HTableDescriptor createTable(String tableName,boolean overWrite) {
		// TODO Auto-generated method stub
	return this.createTable(tableName, null, null, overWrite);//use default column faminily 'cf'
		

	}
	public HTableDescriptor createTable(String tableName,String columnFamily, boolean overWrite) {
		// TODO Auto-generated method stub
	HColumnDescriptor hcd=this.getColumnDescriptor(columnFamily, TIME_TO_LIVE, MAX_VERSION, BLOOM_TYPE, COMPRESSION_ALG);
	ArrayList l=new ArrayList();
	l.add(hcd);
	return this.createTable(tableName, l, null, overWrite);	

	}



	@Override
	public HTableDescriptor createTable(String tableName, HColumnDescriptor hd,
			String startKey, String endKey, int numRegions, boolean overWrite) {
		// TODO Auto-generated method stub
		return null;
	}
	

	
	@Override
	public HTableDescriptor createTable(String tableName, List<HColumnDescriptor> hColDesclist,
			byte[][] splitkeys, boolean overWrite) {
		// TODO Auto-generated method stub
		
		try {
			if(hbaseAdmin.tableExists(tableName)&&!overWrite)
			{
				//table exist and do not drop it
				System.out.println("table exist!do nothing.");
				return getTableDescriptor(tableName);		
			
			}
			else {
				//if(hbaseAdmin.tableExists(tableName))
				this.dropTable(tableName);
				
				HTableDescriptor htd=new HTableDescriptor(tableName);
				
				if((null==hColDesclist)||hColDesclist.size()==0){
					HColumnDescriptor hd=this.getColumnDescriptor("cf", -1, 1, null, null);
					htd.addFamily(hd);			
				}
				else
				{
					//Iterator i=hdlist.iterator();
					for(HColumnDescriptor hdc:hColDesclist)
					{
						htd.addFamily(hdc);
					}
				}
					

				if(null==splitkeys)
					hbaseAdmin.createTable(htd);
				else
					hbaseAdmin.createTable(htd, splitkeys);
				//hbaseAdmin.cre
				
				System.out.println("table "+tableName+" created.");
				return getTableDescriptor(tableName);	
			}
			
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		
	}

	
	public HTableDescriptor getTableDescriptor(String tableName)
	{
		try {
			return hbaseAdmin.getTableDescriptor(Bytes.toBytes(tableName));
		}catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

    public HColumnDescriptor getColumnDescriptor(String columnFamily, int lifetime,
            int maxVersion, BloomType bloomType,Algorithm alg)
    {
    	HColumnDescriptor hd=new HColumnDescriptor(columnFamily);
    	if(null==bloomType)
    		hd.setBloomFilterType(BloomType.ROW);//default ROW
    	else
    		hd.setBloomFilterType(bloomType);
    	if(null==alg)
    	{
    		hd.setCompactionCompressionType(Compression.Algorithm.SNAPPY);
    		hd.setCompressionType(Compression.Algorithm.SNAPPY);
    	}
    	else   
    	{
    		hd.setCompactionCompressionType(alg);//Compression.Algorithm.SNAPPY
    		hd.setCompressionType(alg);
    	}

    	if(lifetime>0)
    		hd.setTimeToLive(lifetime);
    	
    	hd.setMaxVersions(maxVersion>10000?10000:maxVersion);//maxversion 10000
    		
    	
    	return hd;
    }
	@Override
	public boolean dropTable(String tableName) throws Exception {
		// TODO Auto-generated method stub
		if(hbaseAdmin.tableExists(tableName)){
			hbaseAdmin.disableTable(tableName);
			hbaseAdmin.deleteTable(tableName);
			return true;
		}
		else
			return false;
		
	}

	@Override
	public boolean closeTable(HTableInterface htable) {
		// TODO Auto-generated method stub
		try {
			htable.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		return true;
		
	}
	@Override
	public HTableDescriptor createTable(String tableName, byte[][] splitkeys,
			boolean overWrite) {
		// TODO Auto-generated method stub
		HColumnDescriptor hcd=this.getColumnDescriptor("cf", TIME_TO_LIVE, MAX_VERSION, BLOOM_TYPE, COMPRESSION_ALG);
		ArrayList l=new ArrayList();
		l.add(hcd);
		return this.createTable(tableName, l, splitkeys, overWrite);	
		
	}


}


