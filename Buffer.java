package database;

import java.io.RandomAccessFile;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.Date;
import java.text.SimpleDateFormat;

public class Buffer
{
	public int noOfRows; 
	public HashMap<Integer, String[]> content;
	public int[] format; 
	public String[] columnName; 

	
	public Buffer()
	{
		noOfRows = 0;
		content = new HashMap<Integer, String[]>();
	}

	
	public void add(int rowid, String[] val)
	{
		content.put(rowid, val);
		noOfRows = noOfRows + 1;
	}

	
	public void updateFormat()
	{
		for(int i = 0; i < format.length; i++)
			format[i] = columnName[i].length();
		for(String[] i : content.values()){
			for(int j = 0; j < i.length; j++)
				if(format[j] < i[j].length())
					format[j] = i[j].length();
		}
	}

	
	public String fix(int len, String s) 
	{
		return String.format("%-"+(len+3)+"s", s);
	}

	
	public String line(String s,int len) 
	{
		String a = "";
		for(int i=0;i<len;i++) 
		{
			a += s;
		}
		return a;
	}

	public void display(String[] col)
	{
		if(noOfRows == 0)
		{
			System.out.println("");
		}
		else
		{
			
			updateFormat();
			
			if(col[0].equals("*"))
			{
				
				for(int l: format)
					System.out.print(line("-", l+3));
				System.out.println();

				for(int j = 0; j < columnName.length; j++)
					System.out.print(fix(format[j], columnName[j])+"|");
				System.out.println();
				
				for(int l: format)
					System.out.print(line("-", l+3));
				System.out.println();
				
				for(String[] i : content.values()){
					if(i[0].equals("-10000"))
						continue;
					for(int j = 0; j < i.length; j++)
						System.out.print(fix(format[j], i[j])+"|");
					System.out.println();
				}
				System.out.println();
			
			}
			else
			{
				int[] control = new int[col.length];
				for(int j = 0; j < col.length; j++)
					for(int i = 0; i < columnName.length; i++)
						if(col[j].equals(columnName[i]))
							control[j] = i;

				for(int j = 0; j < control.length; j++)
					System.out.print(line("-", format[control[j]]+3));
				System.out.println();
				
				for(int j = 0; j < control.length; j++)
					System.out.print(fix(format[control[j]], columnName[control[j]])+"|");
				System.out.println();
				
				for(int j = 0; j < control.length; j++)
					System.out.print(line("-", format[control[j]]+3));
				System.out.println();
				
				for(String[] i : content.values())
				{
					for(int j = 0; j < control.length; j++)
						System.out.print(fix(format[control[j]], i[control[j]])+"|");
					System.out.println();
				}
				System.out.println();
			}
		}
	}
}