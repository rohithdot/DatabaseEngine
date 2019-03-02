package database;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.util.Scanner;



public class DavisBase {

	
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");

	
	static String currentDB = "user_data";
	
	
	public static void main(String[] args) {
		initializeDB();


		System.out.println(
				"---------------------------------------------------------------------");
		System.out.println("Welcome to DavisBase");
		System.out.println(
				"---------------------------------------------------------------------");

		String query = "";

		while (!query.equals("exit")) 
		{
			System.out.print("davissql> ");
			
			query = scanner.next().replace("\n", " ").replace("\r", "");
			query = query.trim().toLowerCase();
			QueryHandler.parse(query);
		}
		System.out.println("Exiting from DavisBase");

	}

	public static void initializeDB() 
	{
		try {
			File data = new File("data");
			if (!data.exists()) {
				data.mkdir();
				System.out.println("Folder 'data' not yet created, creating 'data' folder ");
			}
			File catalog = new File("data//catalog");
			if (catalog.mkdir()) {
				System.out.println("Folder 'data//catalog' not yet created, Initializing catalog!");
				Commands.initializeDataStore();
			}
			else {

				boolean catalog1 = false;
				boolean catalog2 =false;
				String catalog_columns = "davisbase_columns.tbl";
				String catalog_tables = "davisbase_tables.tbl";
				String[] catalogList = catalog.list();

				for (int i = 0; i < catalogList.length; i++) {
					if (catalogList[i].equals(catalog_columns))
						catalog1 = true;
				}
				if (!catalog1) {
					System.out.println(
							"Table 'davisbase_columns.tbl' not yet created, initializing davisbase_columns");
					System.out.println();
					Commands.initializeDataStore();
				}
				
				for (int i = 0; i < catalogList.length; i++) {
					if (catalogList[i].equals(catalog_tables))
						catalog2 = true;
				}
				if (!catalog2) {
					System.out.println(
							"Table 'davisbase_tables.tbl' not yet created, initializing davisbase_tables");
					System.out.println();
					Commands.initializeDataStore();
				}
			}
		} catch (SecurityException se) {
			System.out.println("Application catalog not created " + se);

		}

	}

	// Check if the table exists
	public static boolean tableExist(String table) {
		boolean tableexists = false;
		
		try {
			File userdatafolder = new File("data//user_data");
			if (userdatafolder.mkdir()) {
				System.out.println("Folder 'data//user_data' not yet created, Initializing user_data!");
				
			}
			String[] usertables;
			usertables = userdatafolder.list();
			for (int i = 0; i < usertables.length; i++) {
				if (usertables[i].equals(table))
					return true;
			}
		} catch (SecurityException se) {
			System.out.println("Unable to create data container directory" + se);
		}

		return tableexists;
	}

	public static String[] parserEquation(String equ) 
	{
		String cmp[] = new String[3];
		String temp[] = new String[2];
		if (equ.contains("=")) {
			temp = equ.split("=");
			cmp[0] = temp[0].trim();
			cmp[1] = "=";
			cmp[2] = temp[1].trim();
		}

		if (equ.contains(">")) {
			temp = equ.split(">");
			cmp[0] = temp[0].trim();
			cmp[1] = ">";
			cmp[2] = temp[1].trim();
		}

		if (equ.contains("<")) {
			temp = equ.split("<");
			cmp[0] = temp[0].trim();
			cmp[1] = "<";
			cmp[2] = temp[1].trim();
		}

		if (equ.contains(">=")) {
			temp = equ.split(">=");
			cmp[0] = temp[0].trim();
			cmp[1] = ">=";
			cmp[2] = temp[1].trim();
		}

		if (equ.contains("<=")) {
			temp = equ.split("<=");
			cmp[0] = temp[0].trim();
			cmp[1] = "<=";
			cmp[2] = temp[1].trim();
		}

		if (equ.contains("<>")) {
			temp = equ.split("<>");
			cmp[0] = temp[0].trim();
			cmp[1] = "<>";
			cmp[2] = temp[1].trim();
		}

		return cmp;
	}
	

}