package org.gmcc.tableLoader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.gmcc.utils.rowkey.AiuMocRowkeyImpl;
import org.gmcc.utils.rowkey.RowkeyFactory;
import org.gmcc.utils.tableadmin.HTableAdminFactoryImpl;
import org.gmcc.utils.tableadmin.TablePartionerManager;

public class CdrLoaderThread extends Thread {

	/**
	 * @param args
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub

		for (int i = 0; i < 4; i++) {
			CdrLoaderThread amlt = new CdrLoaderThread();
			amlt.start();
			Thread.sleep(1000);
		}
		while (CdrLoaderThread.isRunning()) {
			Thread.sleep(1000);
			System.out.println("wait for job to completed!");
		}
		System.out.println("test finish!");
	}

	File F;
	public static List RUNNING_STATE = new ArrayList();// store the state of
														// running threads

	public static long ROW_COUNTER=0;
	public static long SKIPPED_ROW_COUNTER=0;
	
	long rowcounter=0;
	long skippedrowcounter=0;
	
	String hTableName,confFilePath;
	RowkeyFactory rg;
	int NUM_PARTION = 4;
	public CdrLoaderThread(File f, String tableName,RowkeyFactory rf,int numPartion) {
		RUNNING_STATE.add(new Integer(1));
		this.F = f;
		this.hTableName=tableName;
		//this.confFilePath=confFile;
		this.rg=rf;
		this.NUM_PARTION=numPartion;
	}

	public CdrLoaderThread() {

	}

	public void run() {
		
		// System.out.println("Thread sleep 5s!");
		BufferedReader br = null;
		HTableInterface htable=null;
		try {
			// place your job here

			//int NUM_PARTION = 4;
			//int SALT_LEN = 4;
			int BATCH_SIZE=1000;
			

			HTablePool htp = new HTablePool();
			htable = htp.getTable(this.hTableName);
			List<Put> putList = new ArrayList();
			// int counter=0;

		//	AiuMocRowkeyGenerator rg = new AiuMocRowkeyGenerator();
		//	rg.addConfiguration(this.confFilePath);//"/home/training/cdr_aiu_moc_conf.xml"
			String cdr = "";
		//	long s = System.currentTimeMillis();
			
			// int counter=0;

			br = new BufferedReader(new FileReader(F));

			while ((cdr = br.readLine()) != null) {
				// System.out.println(Bytes.toString(rg.genLongRowkey(cdr,
				// 10)).length());
				byte[] rk=rg.genLongRowkey(cdr, NUM_PARTION);
				if(rk==null)
				{	
					skippedrowcounter++;
					continue;				
				}
				Put p = new Put(rk);
				p.add(Bytes.toBytes("cf"), Bytes.toBytes("q"),//comlumn family  'cf' for default, column name 'Q'
						Bytes.toBytes(cdr));
				putList.add(p);
				rowcounter++;
				if (putList.size() > BATCH_SIZE) {
					htable.put(putList);
					htable.flushCommits();
					putList.clear();
				}
			}

			if (putList.size() > 0) {
				htable.put(putList);
				htable.flushCommits();
				putList.clear();
			}

		//	System.out.println((System.currentTimeMillis()-s)+"ms");
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			

			try {
				htable.close();
				br.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			jobCompleted();
			
		}

	}

	public void jobCompleted() {
		// System.out.println("job completed!");
		ROW_COUNTER+=rowcounter;
		SKIPPED_ROW_COUNTER+=skippedrowcounter;
		if (RUNNING_STATE != null && RUNNING_STATE.size() != 0)
			RUNNING_STATE.remove(0);
	}

	public static boolean isRunning() {
		if (RUNNING_STATE.size() != 0)
			return true;
		return false;
	}
	
	public static long getRowCounter()
	{
		return ROW_COUNTER;
	}
	public static long getSkippedRowCounter()
	{
		return SKIPPED_ROW_COUNTER;
	}
}
