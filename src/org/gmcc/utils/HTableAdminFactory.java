package org.gmcc.utils;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.StoreFile;

public interface HTableAdminFactory {

	//HTableInterface getTable(String tableName);
	//HTableDescriptor createTable(String tableName,HColumnDescriptor hd,byte[][]splitkeys,boolean overWrite);
	HTableDescriptor createTable(String tableName, HColumnDescriptor hd,String startKey, 
								String endKey, int numRegions,boolean overWrite) ;
	boolean dropTable(String tableName) throws Exception;
	boolean closeTable(HTableInterface htable);
	HColumnDescriptor getColumnDescriptor(String columnFamily, int lifetime,
    								int maxVersion, StoreFile.BloomType bloomType,Algorithm alg);
	HTableDescriptor createTable(String tableName, boolean overWrite);
	HTableDescriptor createTable(String tableName,
			List<HColumnDescriptor> hdlist, byte[][] splitkeys,
			boolean overWrite);

	
	
	
}
