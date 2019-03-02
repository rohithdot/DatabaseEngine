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

public class CatalogReader
{
	public static final int pageSize = 512;
	public static final String datePattern = "yyyy-MM-dd_HH:mm:ss";
	
	
	
	public static String[] retrievePayload(RandomAccessFile file, long location)
	{
		String[] payload = new String[0];
		try{
			Long tmp;
			SimpleDateFormat fmt = new SimpleDateFormat (datePattern);

			
			file.seek(location);
			int plsize = file.readShort();
			int key = file.readInt();
			int colnum = file.readByte();
			byte[] bt = new byte[colnum];
			int temp = file.read(bt);
			payload = new String[colnum+1];
			payload[0] = Integer.toString(key);
			
			for(int i=1; i <= colnum; i++){
				switch(bt[i-1]){
					case 0x00:  payload[i] = Integer.toString(file.readByte());
								payload[i] = "null";
								break;

					case 0x01:  payload[i] = Integer.toString(file.readShort());
								payload[i] = "null";
								break;

					case 0x02:  payload[i] = Integer.toString(file.readInt());
								payload[i] = "null";
								break;

					case 0x03:  payload[i] = Long.toString(file.readLong());
								payload[i] = "null";
								break;

					case 0x04:  payload[i] = Integer.toString(file.readByte());
								break;

					case 0x05:  payload[i] = Integer.toString(file.readShort());
								break;

					case 0x06:  payload[i] = Integer.toString(file.readInt());
								break;

					case 0x07:  payload[i] = Long.toString(file.readLong());
								break;

					case 0x08:  payload[i] = String.valueOf(file.readFloat());
								break;

					case 0x09:  payload[i] = String.valueOf(file.readDouble());
								break;

					case 0x0A:  tmp = file.readLong();
								Date dateTime = new Date(tmp);
								payload[i] = fmt.format(dateTime);
								break;

					case 0x0B:  tmp = file.readLong();
								Date date = new Date(tmp);
								payload[i] = fmt.format(date).substring(0,10);
								break;

					default:    int len = new Integer(bt[i-1]-0x0C);
								byte[] bytes = new byte[len];
								for(int j = 0; j < len; j++)
									bytes[j] = file.readByte();
								payload[i] = new String(bytes);
								break;
				}
			}

		}
		catch(Exception e)
		{
			System.out.println("Error at retrievePayload");
		}

		return payload;
	}

	

	

	
	public static int calPayloadSize(String table, String[] vals, byte[] bt)
	{
		String[] dt = getDataType(table);
		int size = 1;
		size = size + dt.length - 1;
		for(int i = 1; i < dt.length; i++){
			byte tmp = stcCode(vals[i], dt[i]);
			bt[i - 1] = tmp;
			size = size + fieldLength(tmp);
		}
		return size;
	}

	
	public static short fieldLength(byte bt)
	{
		switch(bt)
		{
			case 0x00: return 1;
			case 0x01: return 2;
			case 0x02: return 4;
			case 0x03: return 8;
			case 0x04: return 1;
			case 0x05: return 2;
			case 0x06: return 4;
			case 0x07: return 8;
			case 0x08: return 4;
			case 0x09: return 8;
			case 0x0A: return 8;
			case 0x0B: return 8;
			default:   return (short)(bt - 0x0C);
		}
	}

	
	public static byte stcCode(String val, String dataType)
	{
		if(val.equals("null"))
		{
			switch(dataType)
			{
				case "TINYINT":     return 0x00;
				case "SMALLINT":    return 0x01;
				case "INT":			return 0x02;
				case "BIGINT":      return 0x03;
				case "REAL":        return 0x02;
				case "DOUBLE":      return 0x03;
				case "DATETIME":    return 0x03;
				case "DATE":        return 0x03;
				case "TEXT":        return 0x03;
				default:			return 0x00;
			}							
		}
		else
		{
			switch(dataType)
			{
				case "TINYINT":     return 0x04;
				case "SMALLINT":    return 0x05;
				case "INT":			return 0x06;
				case "BIGINT":      return 0x07;
				case "REAL":        return 0x08;
				case "DOUBLE":      return 0x09;
				case "DATETIME":    return 0x0A;
				case "DATE":        return 0x0B;
				case "TEXT":        return (byte)(val.length()+0x0C);
				default:			return 0x00;
			}
		}
	}

	public static int searchKey(RandomAccessFile file, int key)
	{
		int val = 1;
		try{
			int numPages = pages(file);
			for(int page = 1; page <= numPages; page++){
				file.seek((page - 1)*pageSize);
				byte pageType = file.readByte();
				if(pageType == 0x0D){
					int[] keys = Page.getKey(file, page);
					if(keys.length == 0)
						return 0;
					int rm = Page.getRt(file, page);
					if(keys[0] <= key && key <= keys[keys.length - 1]){
						return page;
					}else if(rm == 0 && keys[keys.length - 1] < key){
						return page;
					}
				}
			}
		}
		catch(Exception e)
		{
			System.out.println("Error at searchKey");
			System.out.println(e);
		}

		return val;
	}


	public static String[] getDataType(String table)
	{
		String[] dt = new String[0];
		try{
			RandomAccessFile dtfile = new RandomAccessFile("data//catalog//davisbase_columns.tbl", "rw");
			Buffer buffer = new Buffer();
			String[] columnName = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			if(!table.equals("davisbase_columns") && !table.equals("davisbase_tables"))
				table = DavisBase.currentDB+"."+table;
			String[] comp = {"table_name","=",table};
			filter(dtfile, comp, columnName, buffer);
			HashMap<Integer, String[]> content = buffer.content;
			ArrayList<String> array = new ArrayList<String>();
			for(String[] i : content.values()){
				array.add(i[3]);
			}
			dt = array.toArray(new String[array.size()]);
			dtfile.close();
			return dt;
		}
		catch(Exception e)
		{
			System.out.println("Error in getting the data type");
			System.out.println(e);
		}
		return dt;
	}

	public static String[] getColName(String table)
	{
		String[] c = new String[0];
		try{
			RandomAccessFile cnfile = new RandomAccessFile("data//catalog//davisbase_columns.tbl", "rw");
			Buffer buffer = new Buffer();
			String[] name = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			if(!table.equals("davisbase_columns") && !table.equals("davisbase_tables"))
				table = DavisBase.currentDB+"."+table;
			String[] comp = {"table_name","=",table};
			filter(cnfile, comp, name, buffer);
			HashMap<Integer, String[]> content = buffer.content;
			ArrayList<String> array = new ArrayList<String>();
			for(String[] i : content.values()){
				array.add(i[2]);
			}
			c = array.toArray(new String[array.size()]);
			cnfile.close();
			return c;
		}
		catch(Exception e)
		{
			System.out.println("Error in getting the column name");
			System.out.println(e);
		}
		return c;
	}

	public static String[] getNullable(String table)
	{
		String[] n = new String[0];
		try{
			RandomAccessFile file = new RandomAccessFile("data//catalog//davisbase_columns.tbl", "rw");
			Buffer buffer = new Buffer();
			String[] name = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			if(!table.equals("davisbase_columns") && !table.equals("davisbase_tables"))
				table = DavisBase.currentDB+"."+table;
			String[] cmp = {"table_name","=",table};
			filter(file, cmp, name, buffer);
			HashMap<Integer, String[]> content = buffer.content;
			ArrayList<String> list = new ArrayList<String>();
			for(String[] i : content.values())
			{
				list.add(i[5]);
			}
			n = list.toArray(new String[list.size()]);
			file.close();
			return n;
		}catch(Exception e){
			System.out.println("Error at getNullable");
			System.out.println(e);
		}
		return n;
	}

	
	
	
	public static void filter(RandomAccessFile file, String[] cmp, String[] columnName, String[] type, Buffer buffer){
		try{
			int num = pages(file);
			
			for(int page = 1; page <= num; page++)
			{
				file.seek((page-1)*pageSize);
				byte kind = file.readByte();
				if(kind == 0x05)
					continue;
				else{
					byte numCells = Page.getCellNum(file, page);

					for(int i=0; i < numCells; i++){
						
						long loc = Page.getCellLoc(file, page, i);
						file.seek(loc+2); 
						int rowid = file.readInt(); 
						int num_cols = new Integer(file.readByte()); 

						String[] pl = retrievePayload(file, loc);

						for(int j=0; j < type.length; j++)
							if(type[j].equals("DATE") || type[j].equals("DATETIME"))
								pl[j] = "'"+pl[j]+"'";
						
						boolean check = cmpCheck(pl, rowid, cmp, columnName);

						
						for(int j=0; j < type.length; j++)
							if(type[j].equals("DATE") || type[j].equals("DATETIME"))
								pl[j] = pl[j].substring(1, pl[j].length()-1);

						if(check)
							buffer.add(rowid, pl);
					}
				}
			}

			buffer.columnName = columnName;
			buffer.format = new int[columnName.length];

		}
		catch(Exception e)
		{
			System.out.println("Error at filter method");
			e.printStackTrace();
		}

	}

	
	public static void filter(RandomAccessFile file, String[] cmp, String[] columnName, Buffer buffer){
		try{
			int numPages = pages(file);
			
			for(int page = 1; page <= numPages; page++){
				file.seek((page-1)*pageSize);
				byte kind = file.readByte();
				if(kind == 0x05)
					continue;
				else{
					byte numCells = Page.getCellNum(file, page);

					for(int i=0; i < numCells; i++){
						long loc = Page.getCellLoc(file, page, i);
						file.seek(loc+2); 
						int rowid = file.readInt(); 
						int num_cols = new Integer(file.readByte()); 
						String[] pl = retrievePayload(file, loc);

						boolean check = cmpCheck(pl, rowid, cmp, columnName);
						if(check)
							buffer.add(rowid, pl);
					}
				}
			}

			buffer.columnName = columnName;
			buffer.format = new int[columnName.length];

		}catch(Exception e){
			System.out.println("Error while filtering");
			e.printStackTrace();
		}

	}

	
	public static int pages(RandomAccessFile file)
	{
		int num_pages = 0;
		try
		{
			num_pages = (int)(file.length()/(new Long(pageSize)));
		}
		catch(Exception e)
		{
			System.out.println("Error at making InteriorPage");
		}

		return num_pages;
	}

	
	public static boolean cmpCheck(String[] pl, int rowid, String[] cmp, String[] columnName)
	{

		boolean check = false;
		if(cmp.length == 0)
		{
			check = true;
		}
		else{
			int colPos = 1;
			for(int i = 0; i < columnName.length; i++){
				if(columnName[i].equals(cmp[0])){
					colPos = i + 1;
					break;
				}
			}
			String opt = cmp[1];
			String val = cmp[2];
			if(colPos == 1)
			{
				switch(opt)
				{
					case "=": if(rowid == Integer.parseInt(val)) 
								check = true;
							  else
							  	check = false;
							  break;
					case ">": if(rowid > Integer.parseInt(val)) 
								check = true;
							  else
							  	check = false;
							  break;
					case "<": if(rowid < Integer.parseInt(val)) 
								check = true;
							  else
							  	check = false;
							  break;
					case ">=": if(rowid >= Integer.parseInt(val)) 
								check = true;
							  else
							  	check = false;	
							  break;
					case "<=": if(rowid <= Integer.parseInt(val)) 
								check = true;
							  else
							  	check = false;	
							  break;
					case "<>": if(rowid != Integer.parseInt(val))  
								check = true;
							  else
							  	check = false;	
							  break;						  							  							  							
				}
			}else{
				if(val.equals(pl[colPos-1]))
					check = true;
				else
					check = false;
			}
		}
		return check;
	}

	
}


