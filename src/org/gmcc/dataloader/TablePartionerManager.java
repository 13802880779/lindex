package org.gmcc.dataloader;

import org.apache.hadoop.hbase.util.Bytes;

public class TablePartionerManager {

	public static byte[][] getPartionSplitKeys(int numPartions)
	{
		byte[][] SplitKeys=new byte[numPartions-1][];
		for(int i=1; i<numPartions; i++)
		{
			SplitKeys[i-1]=Bytes.toBytes(i);
		}		
		return SplitKeys;
		
	}
}
