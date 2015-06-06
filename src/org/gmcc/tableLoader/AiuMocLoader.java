package org.gmcc.tableLoader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.gmcc.utils.FileMatcher;
import org.gmcc.utils.rowkey.AiuMocRowkeyImpl;
import org.gmcc.utils.tableadmin.HTableAdminFactoryImpl;
import org.gmcc.utils.tableadmin.TablePartionerManager;

public class AiuMocLoader{

	/**
	 * @param args
	 * -t tablename to create or to load to
	 * -p partionNum
	 * -f filepath to load
	 * -c configuration file path
	 * -tn threadNum
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		//create table aiu_moc_
		if(args.length!=10)
		{
			System.out.println("invalid args,usage: -t tableName -p partionNum -f filePath -c confFilePath -tn threadNum");
			System.exit(1);
		}
		String tableName=args[1];
		int partionNum=Integer.parseInt(args[3])>0?Integer.parseInt(args[3]):1;
		String filepath=args[5];
		String conffilepath=args[7];
		int threadNum=Integer.parseInt(args[9])>0?Integer.parseInt(args[9]):1;
		
				
		AiuMocLoader.loadAiuMocCdr(tableName, partionNum, filepath, conffilepath, threadNum);
		//AiuMocLoadThread amt=new AiuMocLoadThread(new File("/home/training/aiu-moc-cdr-201502191555-00001_20150219#20150219160201#_192.168.35.199.dat"),
		//		"aiu_moc_20150219",
		//		"/home/training/cdr_aiu_moc_conf.xml");
		//amt.start();
		
		
		


	}
	/**
	 * @param args
	 * -t tablename to create or to load to
	 * -p partionNum
	 * -f filepath to load
	 * -c configuration file path
	 * -tn threadNum
	 * @throws Exception 
	 */
	public static void loadAiuMocCdr(String tableName,int partionNum,String filepath,String conffilepath,int threadNum) throws Exception
	{
		int SALT_LEN=4;
		//System.out.println(threadNum);
		
		
		HTableAdminFactoryImpl hbaseAdmin=new HTableAdminFactoryImpl();
		hbaseAdmin.createTable(tableName,TablePartionerManager.getStrPartionSplitKeys(partionNum,SALT_LEN), true);
		hbaseAdmin.close();
		
		AiuMocRowkeyImpl rg = new AiuMocRowkeyImpl(conffilepath);
	//	rg.addConfiguration(conffilepath);//"/home/training/cdr_aiu_moc_conf.xml"
		
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
					System.out.println("loading file:"+f.getName());
					
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
					System.out.println("loading file:"+f.getName());
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
