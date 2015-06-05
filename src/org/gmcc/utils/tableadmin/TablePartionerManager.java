package org.gmcc.utils.tableadmin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.gmcc.utils.StrUtils;

public class TablePartionerManager {

	public static byte[][] getStrPartionSplitKeys(int numPartions,int strlen)
	{
		byte[][] SplitKeys=new byte[numPartions-1][];

		for(int i=1; i<numPartions; i++)
		{
			SplitKeys[i-1]=Bytes.toBytes(StrUtils.leftPadWithZero(i, strlen));
			//SplitKeys[i-1]=Bytes.toBytes(i);
		}		
		return SplitKeys;
		
	}
	public static byte[][] getIntPartionSplitKeys(int numPartions)
	{
		byte[][] SplitKeys=new byte[numPartions-1][];
		for(int i=1; i<numPartions; i++)
		{
			SplitKeys[i-1]=Bytes.toBytes(i);
		}		
		return SplitKeys;
		
	}
}
