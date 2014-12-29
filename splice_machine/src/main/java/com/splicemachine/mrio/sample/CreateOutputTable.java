package com.splicemachine.mrio.sample;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class CreateOutputTable {

	public void createTable(String tableName, String sqlStat, String connStr)
	{
		try {
			Class.forName("org.apache.derby.jdbc.ClientDriver");
			Connection conn = DriverManager.getConnection(connStr);
			Statement stmt = conn.createStatement();
			stmt.execute(sqlStat);
			
		} catch (SQLException | ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
	
	public static void main(String[] args) throws Exception {
		
		//arg0: Table name
		//arg1: SQL statment for creating table
		if(args.length < 3)
			throw new Exception("missing argument");
		String tableName = args[0];
		String sqlStat = args[1];
		String connStr = args[2];
		System.out.println(connStr);
		CreateOutputTable cot = new CreateOutputTable();
		cot.createTable(tableName, sqlStat, connStr);
	}

}
