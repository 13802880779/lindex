package org.gmcc.utils;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class StrUtils {

	public static String[] split(String line,String delim)
	{
	//	String line="10.190.174.142-----[03/Dec/2011:13:28:11--0800]-GET-/images/filmmediablock/360/GOEMON-NUKI-000159.jpg-HTTP/1.1-200-";
		int pos_s=0;
		int pos_e=0;
		ArrayList strlist=new ArrayList();
		while((pos_e=line.indexOf(delim, pos_s))!=-1)
		{
			strlist.add(line.substring(pos_s,pos_e));
			pos_s=pos_e+1;
		}
		
		if(pos_s<=line.length())
		{
				strlist.add(line.substring(pos_s,line.length()));
		}
		return (String[]) strlist.toArray(new String[strlist.size()]);
	}
	
	public static String  long2datestr(long t)
	{
		SimpleDateFormat format = new SimpleDateFormat("yyyyMMddhhmmssSSS");
		Date d=new Date(t);
		return format.format(d);	
	}
	
	public static String leftPadWithZero(Object l,int len)
	{
		return String.format("%0"+len+"d", l);
	}
}
