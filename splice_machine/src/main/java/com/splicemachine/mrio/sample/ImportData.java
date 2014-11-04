package com.splicemachine.mrio.sample;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class ImportData {

	String tableName = null;
	Connection conn = null;
	
	public void createTable(String tableName, String sqlStat, String connStr)
	{
		this.tableName = tableName;
		try {
			if(conn == null)
			{
				Class.forName("org.apache.derby.jdbc.ClientDriver");
				conn = DriverManager.getConnection(connStr);	
			}
			Statement stmt = conn.createStatement();
			stmt.execute(sqlStat);
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void insertData(String filePath, String splitCha, String connStr)
	{
		BufferedReader br = null;
		File f = new File(".");
		//String absolutePath = f.getAbsolutePath();
		try {		
			if(conn == null)
			{
				Class.forName("org.apache.derby.jdbc.ClientDriver");
				conn = DriverManager.getConnection(connStr);
			}
				
			Statement stmt = conn.createStatement();
			
			br = new BufferedReader(new FileReader(filePath));
			
			String line = br.readLine();
			
	        while (line != null) {

	            line = line.trim();
	            line = line.replaceAll("'", "");
	            if((!line.equals("")) && (line.matches("[a-zA-Z].*")))
	            {
	            	String query = "insert into "+  
							tableName +" values('"+line+"')";
	            	//System.out.println(query);
	            	stmt.executeUpdate(query);
	            }
	            line = br.readLine();
	        }
			
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally{
			if(br != null)
				try {
					br.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
	}
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		//arg0: Table name
		//arg1: SQL for creating table
		//arg2: Source data file path (relative path to spliceengine/splice_machine/)
		//arg3: Split Character
		if(args.length < 5)
			throw new Exception("missing argument");
		String tableName = args[0];
		String sqlStat = args[1];
		String filePath = args[2];
		String splitCha = args[3];
		String connStr = args[4];
		ImportData impData = new ImportData();
		
		impData.createTable(tableName, sqlStat, connStr);
		impData.insertData(filePath, splitCha, connStr);
		
	}

}
