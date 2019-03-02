package database;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.util.Scanner;


public class QueryHandler{


		public static void parse(String query) 
	{

		String[] substrings = query.split(" ");		

		switch (substrings[0]) 
		{
		
		
		case "create":
			createQuery(query);
			
			break;

		case "insert":
			insertQuery(query);
			
			break;
			
		case "select":
			selectQuery(query);
			
			break;	
			
		case "drop":

			dropQuery(query);

			break;

		case "update":
			updateQuery(query);

			break;

		case "show":
			String tables = substrings[1];
			System.out.println();
			if(tables.equals("tables")){
				Commands.show();
			}
			else{
				System.out.println("\nIncorrect input. Please check the readme.txt file to know the supported commands\n");

			}
			
			break;

	
		case "exit":
			System.out.println();
			break;

		case "version":

			System.out.println("\nDavisBase: Version 1.0\n");
		
			break;

		default:
			
			System.out.println("\nIncorrect input. Please check the readme.txt file to know the supported commands\n");
			break;

		}
	}
	public static void createQuery(String query){
		String[] subs = query.split(" ");
		if(subs[1].equals("table")){
				String tablename = subs[2];
	
				String[] temp = query.split(tablename);
	
				String temp2 = temp[1].trim();
				int length=temp2.length();
				
				if(temp2.charAt(0)=='(' && temp2.charAt(length-1)==')'){
					String[] colnames = temp2.substring(1, temp2.length() - 1).split(",");
					for (int i = 0; i < colnames.length; i++){
						colnames[i] = colnames[i].trim();
					}
					if (!DavisBase.tableExist(tablename)) 
					{
						Commands.createTable(tablename, colnames);
						System.out.println("Table "+tablename+" created successfully.\n");
						
												
					}
					else{
						System.out.println("Table " + tablename + " already exists.\n");
					}
				}
				else{
					System.out.println("Incorrect input. Please check the readme.txt file to know about the supported commands\n");
				}
		}
			
		else{
			System.out.println("Incorrect input. Please check the readme.txt file to know about the supported commands\n");				
		}
	}

	public static void insertQuery(String query){
		String[] subs = query.split(" ");

		String insert_table = subs[2];
		String insert_vals1 = query.split("values")[1].trim();
		insert_vals1 = insert_vals1.substring(1, insert_vals1.length() - 1);
		String[] insert_vals2 = insert_vals1.split(",");
		for (int i = 0; i < insert_vals2.length; i++)
			insert_vals2[i] = insert_vals2[i].trim();
		if (!DavisBase.tableExist(insert_table)) {
			System.out.println("Table " + insert_table + " doesn't exist.\n");
			return;
		}
		RandomAccessFile insertfile;
		try {
			insertfile = new RandomAccessFile("data//user_data//"+insert_table+"//"+insert_table+".tbl", "rw");
			Commands.insertInto(insertfile,insert_table, insert_vals2);
			System.out.println("Inserted Successfully\n");
			insertfile.close();
		} catch (Exception e)
		{
			
			e.printStackTrace();
		}

	}
	public static void selectQuery(String query){
			String[] subs = query.split(" ");
			String[] condition;
			String[] select_column;
			String[] temp = query.split("where");
			String[] temp1 = temp[0].split("from");
			String table = temp1[1].trim();
			String columns = temp1[0].replace("select", "").trim();
			
			if(table.equals("davisbase_tables") || table.equals("davisbase_columns"))
			{
				if (columns.contains("*")) 
				{
					select_column = new String[1];
					select_column[0] = "*";
				} 
				else {
				
					select_column = columns.split(",");
					for (int i = 0; i < select_column.length; i++)
						select_column[i] = select_column[i].trim();
				}
				if (temp.length > 1) {
					String filter = temp[1].trim();
					condition = DavisBase.parserEquation(filter);
				} else {
					condition = new String[0];
				}
				if(table.equals("davisbase_tables")){
					Commands.select("data//catalog//davisbase_tables.tbl", table, select_column, condition);
					System.out.println();
					return;
				}
				else{
					Commands.select("data//catalog//davisbase_columns.tbl", table, select_column, condition);
					System.out.println();
					return;
				}
			}

			else{
				if(!DavisBase.tableExist(table)) {
		
					System.out.println("Table " + table + " doesn't exist.");
					System.out.println("Please enter the correct table name.\n");
					
					return;
				}
			}

			if (temp.length > 1) 
			{
				String filter = temp[1].trim();
				condition = DavisBase.parserEquation(filter);
			} 
			else {
				condition = new String[0];
			}

			if (columns.contains("*")) {
				select_column = new String[1];
				select_column[0] = "*";
			} else {
				select_column = columns.split(",");
				for (int i = 0; i < select_column.length; i++)
					select_column[i] = select_column[i].trim();
			}
			
			Commands.select("data//user_data//"+table+"//"+table+".tbl", table, select_column, condition);
			System.out.println();
	}

	public static void updateQuery(String query){
			String[] subs = query.split(" ");

			String updateTable = subs[1];
			String[] update_temp1 = query.split("set");
			String[] update_temp2 = update_temp1[1].split("where");
			String where = update_temp2[1];
			String set = update_temp2[0];
			String[] filteredset = DavisBase.parserEquation(set);
			String[] filteredwhere = DavisBase.parserEquation(where);
			if (!DavisBase.tableExist(updateTable)) 
			{
				System.out.println("Table " + updateTable + " does not exist.\n");
				
				return;
			}
			Commands.update(updateTable, filteredset, filteredwhere);
			System.out.println("Table "+updateTable+" updated successfully.\n");
			
			

	}
	public static void dropQuery(String query){
			String[] subs = query.split(" ");

			if(subs[1].equals("table")){
				String dropTable = subs[2];
				if (DavisBase.tableExist(dropTable)) 
				{	
					Commands.drop(dropTable,"user_data");
					System.out.println("Table "+dropTable+" dropped successfully.\n");
					
				}
				else{
					System.out.println("Table " + dropTable + " does not exist.\n");
					
					
				}
				
			}
			else{
				System.out.println("Incorrect input. Please check the readme.txt file to know the supported commands\n");
			}
			
	}
}