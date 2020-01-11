# DatabaseEngine
Database engine to perform basic DDL, DML, VDL commands from the command prompt similar to MySQL

The Project was developed using Eclipse IDE for Java.

Instructions for running the app:

From eclipse:

1) Extract the compressed folder downloaded from eLearning. 
2) Import the folder into the Eclipse IDE.
3) Run the DavisBase.java to start the application.

 (or)

From local terminal:
4) Extract the compressed folder DavisBase
5) Open terminal or command prompt
6) Navigate to the path where the folder has been extracted.
7) Change directory to the DavisBase\src\database in terminal and compile all files using the below command:
      javac *.java
8) Change directory to parent directory (DavisBase\src)
9) Run DavisBase.class file using the below command:
     java database.DavisBase


The following commands are supported:

(a)SHOW TABLES;                             		           		   			 Displays a list of all tables in DavisBase
(b)CREATE TABLE <table_name> <column list>;                        		       	 Creates a new table 
(c)DROP TABLE <table_name>;                                     		  			 Remove a table schema, and all of its contained data
(d)INSERT INTO table_name [column_list] VALUES value_list;        			 Inserts a single record into a table
(e)UPDATE table_name SET column_name = value WHERE [condition];  		 Modifies one or more records in a table
(f)SELECT * FROM <table_name>; 						    		 Display all records in the table
(g)SELECT * FROM <table_name> WHERE rowid = <value>;				 Display records satisfying a particular condition
(h) exit;											 To exit from the davisbase
