package org.gmcc.tableLoader;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.gmcc.utils.FileMatcher;
import org.gmcc.utils.rowkey.AiuMocRowkeyImpl;
import org.gmcc.utils.rowkey.RowkeyFactory;
import org.gmcc.utils.tableadmin.HTableAdminFactoryImpl;
import org.gmcc.utils.tableadmin.TablePartionerManager;

public class CdrLoader {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	public static void loadCdr(String tableName,
			boolean overwrite,
			int partionNum,
			String filepath,
			RowkeyFactory rg,
			int threadNum) throws Exception
	{
		
		//System.out.println(threadNum);
		//AiuMocRowkeyGenerator rg = new AiuMocRowkeyGenerator();
		//rg.addConfiguration(confpath);//"/home/training/cdr_aiu_moc_conf.xml"
		int SALT_LEN=rg.SALT_LEN;
		
		HTableAdminFactoryImpl hbaseAdmin=new HTableAdminFactoryImpl();
		hbaseAdmin.createTable(tableName,TablePartionerManager.getStrPartionSplitKeys(partionNum,SALT_LEN), overwrite);
		hbaseAdmin.close();
		

		
		List<File> filelist=new ArrayList();
		FileMatcher.matchFiles(filepath, filelist);
		
		long stime=System.currentTimeMillis();
		if(filelist.size()!=0)
		{
			
			int loopnum=filelist.size()/threadNum;
			//System.out.println("loop count:"+loopnum);
			
			for(int i=0; i<loopnum;i++)
			{
				//System.out.println((new Date().toString())+"==>loop num:"+i);
				
				for(int j=0;j<threadNum; j++)
				{
					
					int idx=i*threadNum+j;
				//	System.out.println("file index:"+idx);
					File f=(File)filelist.get(idx);
					System.out.println("loading file to "+tableName+":"+f.getName());
					
					CdrLoaderThread amt=new CdrLoaderThread(f,tableName,rg,partionNum);
					amt.start();
				}
				//Thread.sleep(2000);
				
				while (CdrLoaderThread.isRunning()) {
					Thread.sleep(100);
					//System.out.println("wait for threads to completed!");
				}
				
			}
			
			int last=filelist.size()%threadNum;
			if(last!=0)
			{
				for(int i=0; i<last; i++)
				{
					int idx=loopnum*threadNum+i;
					File f=(File)filelist.get(idx);
					System.out.println("loading file to "+tableName+":"+f.getName());
				//	AiuMocLoadThread amt=new AiuMocLoadThread(f,tableName,rg);
					CdrLoaderThread amt=new CdrLoaderThread(f,tableName,rg,partionNum);
					amt.start();
				}
				while (CdrLoaderThread.isRunning()) {
					Thread.sleep(100);
					//System.out.println("wait for threads to completed!");
				}
			}
			
			
		}
		
		System.out.println("load file to hbase table "+tableName+" finished! \nused time:"+(System.currentTimeMillis()-stime)+"ms\nrow count:"+CdrLoaderThread.getRowCounter());

	}
}
