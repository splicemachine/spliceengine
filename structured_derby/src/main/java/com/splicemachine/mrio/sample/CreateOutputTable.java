package com.splicemachine.mrio.sample;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class CreateOutputTable {

	public void createTable(String tableName, String sqlStat)
	{
		try {
			Connection conn = DriverManager.getConnection("jdbc:splice://localhost:1527/splicedb;user=splice;password=admin");
			Statement stmt = conn.createStatement();
			stmt.execute(sqlStat);
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		//arg0: Table name
		//arg1: SQL statment for creating table
		if(args.length < 2)
			throw new Exception("missing argument");
		String tableName = args[0];
		String sqlStat = args[1];
		CreateOutputTable cot = new CreateOutputTable();
		cot.createTable(tableName, sqlStat);
	}

}
