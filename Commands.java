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


public class Commands{

	private static RandomAccessFile davisbaseTablesCatalog;
	private static RandomAccessFile davisbaseColumnsCatalog;
	public static final int pageSize = 512;

	public static void show()
	{
			String[] columns = {"table_name"};
			String[] sample = new String[0];
			
			
			select("data//catalog//davisbase_tables.tbl","davisbase_tables", columns, sample);
	}
	public static void drop(String table,String db)
	{
			try{
				
				RandomAccessFile dropfile = new RandomAccessFile("data//catalog//davisbase_tables.tbl", "rw");
				int num = CatalogReader.pages(dropfile);
				for(int page = 1; page <= num; page ++)
				{
					dropfile.seek((page-1)*pageSize);
					byte kind= dropfile.readByte();
					if(kind== 0x05)
						continue;
					else{
						short[] cells = Page.getCellArr(dropfile, page);
						int i = 0;
						for(int j = 0; j < cells.length; j++){
							long location = Page.getCellLoc(dropfile, page, j);
							String[] pl = CatalogReader.retrievePayload(dropfile, location);
							String tb = pl[1];
							if(!tb.equals(DavisBase.currentDB+"."+table)){
								Page.setCellOffset(dropfile, page, i, cells[j]);
								i++;
							}
						}
						Page.setCellNum(dropfile, page, (byte)i);
					}
				}

				
				dropfile = new RandomAccessFile("data//catalog//davisbase_columns.tbl", "rw");
				num = CatalogReader.pages(dropfile);
				for(int page = 1; page <= num; page ++){
					dropfile.seek((page-1)*pageSize);
					byte kind = dropfile.readByte();
					if(kind == 0x05)
						continue;
					else{
						short[] cells = Page.getCellArr(dropfile, page);
						int i = 0;
						for(int j = 0; j < cells.length; j++){
							long location = Page.getCellLoc(dropfile, page, j);
							String[] pl = CatalogReader.retrievePayload(dropfile, location);
							String tb = pl[1];
							if(!tb.equals(DavisBase.currentDB+"."+table))
							{
								Page.setCellOffset(dropfile, page, i, cells[j]);
								i++;
							}
						}
						Page.setCellNum(dropfile, page, (byte)i);
					}
				}
				dropfile.close();
				
				File drop = new File("data//"+db+"//"+table);
				String[] lf = drop.list();
				for(String f:lf){
					File dropFile = new File("data//"+db+"//"+table,f);
					dropFile.delete();
				}
				drop = new File("data//"+db, table); 
				drop.delete();
			}
			catch(Exception e)
			{
				System.out.println("error during drop");
				System.out.println(e);
			}

	}
	public static void createTable(String table, String[] col)
	{
			try{	
				
				File catalog = new File("data//"+DavisBase.currentDB+"//"+table);
				
				catalog.mkdir();
				RandomAccessFile createfile = new RandomAccessFile("data//"+DavisBase.currentDB+"//"+table+"//"+table+".tbl", "rw");
				createfile.setLength(pageSize);
				createfile.seek(0);
				createfile.writeByte(0x0D);
				createfile.close();
				
				
				createfile = new RandomAccessFile("data//catalog//davisbase_tables.tbl", "rw");
				int num = CatalogReader.pages(createfile);
				int page = 1;
				for(int r = 1; r <= num; r++)
				{
					int rightmost = Page.getRt(createfile, r);
					if(rightmost == 0)
						page = r;
				}
				int[] keys = Page.getKey(createfile, page);
				int l = keys[0];
				for(int i = 0; i < keys.length; i++)
					if(l < keys[i])
						l = keys[i];
				createfile.close();
				String[] values = {Integer.toString(l+1), DavisBase.currentDB+"."+table};
				insertInto("davisbase_tables", values);

				RandomAccessFile catalogfile = new RandomAccessFile("data//catalog//davisbase_columns.tbl", "rw");
				Buffer buffer = new Buffer();
				String[] columnName = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
				String[] comparision = {};
				CatalogReader.filter(catalogfile, comparision, columnName, buffer);
				l = buffer.content.size();

				for(int i = 0; i < col.length; i++){
					l = l + 1;
					String[] sub = col[i].split(" ");
					String n = "YES";
					if(sub.length > 2)
						n = "NO";
					String col_name = sub[0];
					String dt = sub[1].toUpperCase();
					String pos = Integer.toString(i+1);
					String[] v = {Integer.toString(l), DavisBase.currentDB+"."+table, col_name, dt, pos, n};
					insertInto("davisbase_columns", v);
				}
				catalogfile.close();
				createfile.close();
			}catch(Exception e){
				System.out.println("Error at createTable");
				e.printStackTrace();
			}
	}
	public static void update(String table, String[] set, String[] cmp)
	{
			try{
				List<Integer> key = new ArrayList<Integer>();
			
				RandomAccessFile file = new RandomAccessFile("data//"+DavisBase.currentDB+"//"+table+"//"+table+".tbl", "rw");
				
				Buffer buffer = new Buffer();
				String[] columnName = CatalogReader.getColName(table);
				String[] type = CatalogReader.getDataType(table);
				CatalogReader.filter(file, cmp, columnName, type, buffer);
				
				for(String[] i : buffer.content.values()){
					
					for(int j = 0; j < i.length; j++)
						if(buffer.columnName[j].equals(cmp[0]) && i[j].equals(cmp[2])){
							key.add(Integer.parseInt(i[0]));							
							break;
						}
				
				}
					
				
				for(int n:key){
				
					int num = CatalogReader.pages(file);
					int page = 1;
		
					for(int p = 1; p <= num; p++)
						if(Page.hasKey(file, p, n)){
							page = p;
						}
					int[] array = Page.getKey(file, page);
					int id = 0;
					for(int i = 0; i < array.length; i++)
						if(array[i] == n)
							id = i;
					int offset = Page.getCellOffset(file, page, id);
					long location = Page.getCellLoc(file, page, id);
					String[] array_s = CatalogReader.getColName(table);
					int num_cols = array_s.length - 1;
					String[] values = CatalogReader.retrievePayload(file, location);
		
		
					
					for(int i=0; i < type.length; i++)
						if(type[i].equals("DATE") || type[i].equals("DATETIME"))
							values[i] = "'"+values[i]+"'";
		
		
					
					for(int i = 0; i < array_s.length; i++)
						if(array_s[i].equals(set[0]))
							id = i;
					values[id] = set[2];
		
					
					String[] nullable = CatalogReader.getNullable(table);
		
					for(int i = 0; i < nullable.length; i++){
						if(values[i].equals("null") && nullable[i].equals("NO")){
							System.out.println("NULL value constraint violation");
							System.out.println();
							return;
						}
					}
		
					byte[] bt = new byte[array_s.length-1];
					int pls = CatalogReader.calPayloadSize(table, values, bt);
					Page.upLfCell(file, page, offset, pls, n, bt, values,table);
				}
				file.close();

			}catch(Exception e){
				System.out.println("Error at update");
				System.out.println(e);
			}
	}
	public static void insertInto(RandomAccessFile file, String table, String[] values)
	{
			String[] dt = CatalogReader.getDataType(table);
			String[] nullable = CatalogReader.getNullable(table);

			for(int i = 0; i < nullable.length; i++)
				if(values[i].equals("null") && nullable[i].equals("NO")){
					System.out.println("NULL constraint violation");
					
					return;
				}


			int sk = new Integer(values[0]);
			int pageno = CatalogReader.searchKey(file, sk);
			if(pageno != 0)
				if(Page.hasKey(file, pageno, sk))
				{
					System.out.println("Uniqueness constraint violation");
					System.out.println();
					return;
				}
			if(pageno == 0)
				pageno = 1;


			byte[] bt = new byte[dt.length-1];
			short plSize = (short)(CatalogReader.calPayloadSize(table, values, bt));
			int cellSize = plSize + 6;
			int offset = Page.checkLeafSpace(file, pageno, cellSize);

			if(offset != -1)
			{
				Page.insertLfCell(file, pageno, offset, plSize, sk, bt, values,table);
				
			}
			else
			{
				Page.splitlf(file, pageno);
				insertInto(file, table, values);
			}
	}
	public static void insertInto(String table, String[] values)
	{
			try
			{
				RandomAccessFile insertfile = new RandomAccessFile("data//catalog//"+table+".tbl", "rw");
				insertInto(insertfile, table, values);
				insertfile.close();

			}
			catch(Exception e)
			{
				System.out.println("Error while inserting the data");
				e.printStackTrace();
			}
	}
	public static void select(String file, String table, String[] cols, String[] comp)
	{
			try
			{
				Buffer buffer = new Buffer();
				String[] colName = CatalogReader.getColName(table);
				String[] dt = CatalogReader.getDataType(table);

				RandomAccessFile rFile = new RandomAccessFile(file, "rw");
				CatalogReader.filter(rFile, comp, colName, dt, buffer);
				buffer.display(cols);
				rFile.close();
			}
			catch(Exception e)
			{
				System.out.println("Error at select");
				System.out.println(e);
			}
	}


	public static void initializeDataStore() 
	{

			
			try {
				File catalog = new File("data//catalog");
				
				String[] oldfiles;
				oldfiles = catalog.list();
				for (int i=0; i<oldfiles.length; i++) 
				{
					File anOldFile = new File(catalog, oldfiles[i]); 
					anOldFile.delete();
				}
			}
			catch (SecurityException se) 
			{
				System.out.println("Unable to create catalog directory :"+se);
				
			}

			try {
				davisbaseTablesCatalog = new RandomAccessFile("data//catalog//davisbase_tables.tbl", "rw");
				davisbaseTablesCatalog.setLength(pageSize);
				davisbaseTablesCatalog.seek(0);
				davisbaseTablesCatalog.write(0x0D);
				davisbaseTablesCatalog.write(0x02);
				int[] offset=new int[2];
				int size1=24;
				int size2=25;
				offset[0]=pageSize-size1;
				offset[1]=offset[0]-size2;
				davisbaseTablesCatalog.writeShort(offset[1]);
				davisbaseTablesCatalog.writeInt(0);
				davisbaseTablesCatalog.writeInt(10);
				davisbaseTablesCatalog.writeShort(offset[1]);
				davisbaseTablesCatalog.writeShort(offset[0]);
				davisbaseTablesCatalog.seek(offset[0]);
				davisbaseTablesCatalog.writeShort(20);
				davisbaseTablesCatalog.writeInt(1); 
				davisbaseTablesCatalog.writeByte(1);
				davisbaseTablesCatalog.writeByte(28);
				davisbaseTablesCatalog.writeBytes("davisbase_tables");
				davisbaseTablesCatalog.seek(offset[1]);
				davisbaseTablesCatalog.writeShort(21);
				davisbaseTablesCatalog.writeInt(2); 
				davisbaseTablesCatalog.writeByte(1);
				davisbaseTablesCatalog.writeByte(29);
				davisbaseTablesCatalog.writeBytes("davisbase_columns");
			}
			catch (Exception e) 
			{
				System.out.println("Unable to create the database_tables file");
				System.out.println(e);
			}
			
			try 
			{
				davisbaseColumnsCatalog = new RandomAccessFile("data//catalog//davisbase_columns.tbl", "rw");
				davisbaseColumnsCatalog.setLength(pageSize);
				davisbaseColumnsCatalog.seek(0);       
				davisbaseColumnsCatalog.writeByte(0x0D); 
				davisbaseColumnsCatalog.writeByte(0x08); 
				int[] offset=new int[10];
				offset[0]=pageSize-43;
				offset[1]=offset[0]-47;
				offset[2]=offset[1]-44;
				offset[3]=offset[2]-48;
				offset[4]=offset[3]-49;
				offset[5]=offset[4]-47;
				offset[6]=offset[5]-57;
				offset[7]=offset[6]-49;
				offset[8]=offset[7]-49;
				davisbaseColumnsCatalog.writeShort(offset[8]); 
				davisbaseColumnsCatalog.writeInt(0); 
				davisbaseColumnsCatalog.writeInt(0); 
				
				for(int i=0;i<9;i++)
					davisbaseColumnsCatalog.writeShort(offset[i]);

				
				davisbaseColumnsCatalog.seek(offset[0]);
				davisbaseColumnsCatalog.writeShort(33); 
				davisbaseColumnsCatalog.writeInt(1); 
				davisbaseColumnsCatalog.writeByte(5);
				davisbaseColumnsCatalog.writeByte(28);
				davisbaseColumnsCatalog.writeByte(17);
				davisbaseColumnsCatalog.writeByte(15);
				davisbaseColumnsCatalog.writeByte(4);
				davisbaseColumnsCatalog.writeByte(14);
				
				davisbaseColumnsCatalog.writeBytes("davisbase_tables"); 
				davisbaseColumnsCatalog.writeBytes("rowid"); 
				davisbaseColumnsCatalog.writeBytes("INT"); 
				davisbaseColumnsCatalog.writeByte(1); 
				davisbaseColumnsCatalog.writeBytes("NO"); 
				
				
				davisbaseColumnsCatalog.seek(offset[1]);
				davisbaseColumnsCatalog.writeShort(39); 
				davisbaseColumnsCatalog.writeInt(2); 
				davisbaseColumnsCatalog.writeByte(5);
				davisbaseColumnsCatalog.writeByte(28);
				davisbaseColumnsCatalog.writeByte(22);
				davisbaseColumnsCatalog.writeByte(16);
				davisbaseColumnsCatalog.writeByte(4);
				davisbaseColumnsCatalog.writeByte(14);
				
				davisbaseColumnsCatalog.writeBytes("davisbase_tables"); 
				davisbaseColumnsCatalog.writeBytes("table_name");  
				davisbaseColumnsCatalog.writeBytes("TEXT"); 
				davisbaseColumnsCatalog.writeByte(2); 
				davisbaseColumnsCatalog.writeBytes("NO"); 
				
				
				davisbaseColumnsCatalog.seek(offset[2]);
				davisbaseColumnsCatalog.writeShort(34); 
				davisbaseColumnsCatalog.writeInt(3); 
				davisbaseColumnsCatalog.writeByte(5);
				davisbaseColumnsCatalog.writeByte(29);
				davisbaseColumnsCatalog.writeByte(17);
				davisbaseColumnsCatalog.writeByte(15);
				davisbaseColumnsCatalog.writeByte(4);
				davisbaseColumnsCatalog.writeByte(14);
				
				davisbaseColumnsCatalog.writeBytes("davisbase_columns");
				davisbaseColumnsCatalog.writeBytes("rowid");
				davisbaseColumnsCatalog.writeBytes("INT");
				davisbaseColumnsCatalog.writeByte(1);
				davisbaseColumnsCatalog.writeBytes("NO");
				
				
				davisbaseColumnsCatalog.seek(offset[3]);
				davisbaseColumnsCatalog.writeShort(40); 
				davisbaseColumnsCatalog.writeInt(4); 
				davisbaseColumnsCatalog.writeByte(5);
				davisbaseColumnsCatalog.writeByte(29);
				davisbaseColumnsCatalog.writeByte(22);
				davisbaseColumnsCatalog.writeByte(16);
				davisbaseColumnsCatalog.writeByte(4);
				davisbaseColumnsCatalog.writeByte(14);
				
				davisbaseColumnsCatalog.writeBytes("davisbase_columns");
				davisbaseColumnsCatalog.writeBytes("table_name");
				davisbaseColumnsCatalog.writeBytes("TEXT");
				davisbaseColumnsCatalog.writeByte(2);
				davisbaseColumnsCatalog.writeBytes("NO");
				
				
				davisbaseColumnsCatalog.seek(offset[4]);
				davisbaseColumnsCatalog.writeShort(41); 
				davisbaseColumnsCatalog.writeInt(5); 
				davisbaseColumnsCatalog.writeByte(5);
				davisbaseColumnsCatalog.writeByte(29);
				davisbaseColumnsCatalog.writeByte(23);
				davisbaseColumnsCatalog.writeByte(16);
				davisbaseColumnsCatalog.writeByte(4);
				davisbaseColumnsCatalog.writeByte(14);
				
				davisbaseColumnsCatalog.writeBytes("davisbase_columns");
				davisbaseColumnsCatalog.writeBytes("column_name");
				davisbaseColumnsCatalog.writeBytes("TEXT");
				davisbaseColumnsCatalog.writeByte(3);
				davisbaseColumnsCatalog.writeBytes("NO");
				
				
				davisbaseColumnsCatalog.seek(offset[5]);
				davisbaseColumnsCatalog.writeShort(39); 
				davisbaseColumnsCatalog.writeInt(6); 
				davisbaseColumnsCatalog.writeByte(5);
				davisbaseColumnsCatalog.writeByte(29);
				davisbaseColumnsCatalog.writeByte(21);
				davisbaseColumnsCatalog.writeByte(16);
				davisbaseColumnsCatalog.writeByte(4);
				davisbaseColumnsCatalog.writeByte(14);
				davisbaseColumnsCatalog.writeBytes("davisbase_columns");
				davisbaseColumnsCatalog.writeBytes("data_type");
				davisbaseColumnsCatalog.writeBytes("TEXT");
				davisbaseColumnsCatalog.writeByte(4);
				davisbaseColumnsCatalog.writeBytes("NO");
				
				
				davisbaseColumnsCatalog.seek(offset[6]);
				davisbaseColumnsCatalog.writeShort(49); 
				davisbaseColumnsCatalog.writeInt(7); 
				davisbaseColumnsCatalog.writeByte(5);
				davisbaseColumnsCatalog.writeByte(29);
				davisbaseColumnsCatalog.writeByte(28);
				davisbaseColumnsCatalog.writeByte(19);
				davisbaseColumnsCatalog.writeByte(4);
				davisbaseColumnsCatalog.writeByte(14);
				
				davisbaseColumnsCatalog.writeBytes("davisbase_columns");
				davisbaseColumnsCatalog.writeBytes("ordinal_position");
				davisbaseColumnsCatalog.writeBytes("TINYINT");
				davisbaseColumnsCatalog.writeByte(5);
				davisbaseColumnsCatalog.writeBytes("NO");
								
				davisbaseColumnsCatalog.seek(offset[7]);
				davisbaseColumnsCatalog.writeShort(41); 
				davisbaseColumnsCatalog.writeInt(8); 
				davisbaseColumnsCatalog.writeByte(5);
				davisbaseColumnsCatalog.writeByte(29);
				davisbaseColumnsCatalog.writeByte(23);
				davisbaseColumnsCatalog.writeByte(16);
				davisbaseColumnsCatalog.writeByte(4);
				davisbaseColumnsCatalog.writeByte(14);
				davisbaseColumnsCatalog.writeBytes("davisbase_columns");
				davisbaseColumnsCatalog.writeBytes("is_nullable");
				davisbaseColumnsCatalog.writeBytes("TEXT");
				davisbaseColumnsCatalog.writeByte(6);
				davisbaseColumnsCatalog.writeBytes("NO");
			}
			catch (Exception e) 
			{
				System.out.println("Unable to create the database_columns file");
				System.out.println(e);
			}
	}

}