package com.splicemachine.derby.test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.splicemachine.derby.utils.SpliceUtils;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.rules.TestWatchman;
import org.junit.runners.model.FrameworkMethod;

import com.splicemachine.utils.SpliceLogUtils;

public class DerbyTestRule extends TestWatchman{
	public final Logger LOG;
	public final Map<String,String> tableSchemas;
	private static Connection conn;
	
	private List<Statement> statements = new ArrayList<Statement>();
	private List<ResultSet> resultSets = new ArrayList<ResultSet>();
	private boolean manageTables;
	
	public static void start() throws Exception{
		SpliceDerbyTest.startConnection();
		conn = SpliceDerbyTest.conn;
	}
	
	public static void shutdown() throws Exception {
		SpliceDerbyTest.stopConnection();
	}
	
	public DerbyTestRule(Map<String,String> tableSchemas,Logger LOG){
		this(tableSchemas,true,LOG);
	}
			
	public DerbyTestRule(Map<String,String> tableSchemas,boolean manageTables,Logger LOG){
		this.tableSchemas = tableSchemas;
		this.LOG = LOG;
		this.manageTables = manageTables;
	}
	
	@Override
	public void starting(FrameworkMethod method) {
		LOG.info("Setting up test "+method.getName());
		if(manageTables){
			try {
				createTables();
			} catch (SQLException e) {
				LOG.error("Unable to create tables",e);
				Assert.fail("Unable to create tables! "+e.getMessage());
			}
		}
		super.starting(method);
		LOG.debug("Test setup, starting");
	}

	@Override
	public void finished(FrameworkMethod method) {
		LOG.debug("test "+method.getName()+" finished, closing resources");
		for(ResultSet rs: resultSets){
			try{
				if(!rs.isClosed())
					rs.close();
			}catch(SQLException se){
				LOG.error("Unable to close all connections: "+ se.getMessage());
			}
		}
		for(Statement s : statements){
			try{
				if(!s.isClosed())
					s.close();
			}catch (SQLException se){
				LOG.error("Unable to close all connections: "+ se.getMessage());
			}
		}
		if(manageTables){
			try {
				dropTables();
			} catch (SQLException e) {
				LOG.error("Unable to drop table, this may cause problems: "+e.getMessage());
			}
		}
		super.finished(method);
		LOG.info("test "+ method.getName()+" finished");
	}

	public Statement getStatement() throws SQLException {
		Statement s = conn.createStatement();
		statements.add(s);
		return s;
	}
	
	public ResultSet executeQuery(String sql) throws SQLException {
		Statement s = getStatement();
		ResultSet rs = s.executeQuery(sql);
		resultSets.add(rs);
		return rs;
	}
	
	public void dropTables() throws SQLException{
		java.sql.Statement s = null;
		try{
			s = conn.createStatement();
			for(String tableName:tableSchemas.keySet()){
				dropTable(s,tableName);
			}
			conn.commit();
		}finally{
			if(s!=null)s.close();
		}
	}
	
	public  void createTables() throws SQLException{
		java.sql.Statement s = null;
		try{
			s = conn.createStatement();
			for(String tableName : tableSchemas.keySet()){
				SpliceLogUtils.trace(LOG,"create table %s (%s)",tableName,tableSchemas.get(tableName));
				createTable(s,tableName,tableSchemas.get(tableName));
			}
			conn.commit();
		}finally{
			if(s!=null)s.close();
		}
	}
	
	public static void createTable(java.sql.Statement s,String tableName,String tableSchema) throws SQLException{
		s.execute("create table "+ tableName+" ("+tableSchema+")");
	}

	public void createTable(String tableName,String tableSchema) throws SQLException{
		Statement s = getStatement();
		s.execute("create table "+ tableName+" ("+tableSchema+")");
		conn.commit();
		if(s!=null)s.close();
	}
	
	public static void dropTable(java.sql.Statement s, String tableName) throws SQLException{
		s.execute("drop table "+ tableName);
	}
	
	public void dropTable(String tableName) throws SQLException{
		getStatement().execute("drop table "+tableName);
	}
	

	public void commit() throws SQLException {
		conn.commit();
	}

	public void rollback() throws SQLException {
		conn.rollback();
	}

	public void setAutoCommit(boolean autoCommit) throws SQLException {
		conn.setAutoCommit(autoCommit);
	}

	public PreparedStatement prepareStatement(String sql) throws SQLException {
		PreparedStatement ps = conn.prepareStatement(sql);
		statements.add(ps);
		return ps;
	}

	public Connection getConnection() {
		return conn;
	}

    public void splitLastTable() throws Exception{
       SpliceDerbyTest.splitLastTable();
    }

   public void splitTable(String tableName) throws Exception{
       /*
        * This is a needlessly-complicated and annoying way of doing this,
        * because *when it was written*, the metadata information was kind of all messed up
        * and doing a join between systables and sysconglomerates resulted in an error. When you are
        * looking at this code and going WTF?!? feel free to try cleaning up the SQL. If you get a bunch of
        * wonky errors, then we haven't fixed the underlying issue yet. If you don't, then you just cleaned up
        * some ugly-ass code. Good luck to you.
        *
        */
        ResultSet rs = executeQuery("select tableid from sys.systables where tablename = '"+tableName.toUpperCase()+"'");

       if(rs.next()){
           String tableid = rs.getString(1);

           rs.close();
           rs = executeQuery("select * from sys.sysconglomerates where tableid='"+tableid+"'");
           if(rs.next()){
               long conglomId = rs.getLong(3);
               LOG.info("Splitting table "+conglomId);
               SpliceUtils.splitConglomerate(conglomId);
           }else{
               LOG.info("WHAT?!");
           }

       }else{
           LOG.warn("Unable to split table "+tableName+", conglomeration information could not be found");
       }
    }
}
