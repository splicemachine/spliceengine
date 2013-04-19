package com.splicemachine.derby.test.framework;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import com.google.common.io.Closeables;
import com.splicemachine.constants.HBaseConstants;
import com.splicemachine.derby.utils.ConglomerateUtils;
import com.splicemachine.utils.SpliceLogUtils;


public class SpliceWatcher extends TestWatcher {
	private static final Logger LOG = Logger.getLogger(SpliceWatcher.class);
	private List<Connection> connections = new ArrayList<Connection>();
	private Connection currentConnection;
	private List<Statement> statements = new ArrayList<Statement>();
	private List<ResultSet> resultSets = new ArrayList<ResultSet>();
	
	public SpliceWatcher() {
	
	}

	public Connection getOrCreateConnection() throws Exception {
		if (currentConnection == null || !currentConnection.isValid(10))
			createConnection();
		return currentConnection;
	}

	
	public Connection createConnection() throws Exception {
		currentConnection = SpliceNetConnection.getConnection();
		connections.add(currentConnection);
		return currentConnection;
	}
	
	public PreparedStatement prepareStatement(String sql) throws Exception {
		PreparedStatement ps = getOrCreateConnection().prepareStatement(sql);
		statements.add(ps);
		return ps;
	}
		
	public void closeConnections() {
		try {
			for (Connection connection: connections) {
				if (connection != null && !connection.isClosed()) {
					try {
						connection.close();
					} 	
					catch (SQLException e) {
						connection.rollback();
						connection.close();
					}
				}
			}
		} 
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
    private void closeStatements() {
        try {
	    	for(Statement s : statements){
	        	if(!s.isClosed())
	        		s.close();
	        }
        } catch (Exception e) {
        	throw new RuntimeException(e);
        }
    }

    private void closeResultSets() {
        try {
	    	for(ResultSet r : resultSets){
	        	if(!r.isClosed())
	        		r.close();
	        }
    	} catch (Exception e) {
    		throw new RuntimeException(e);
    	}
    }

	@Override
	protected void starting(Description description) {
		super.starting(description);
	}

	@Override
	protected void finished(Description description) {
		closeAll();
		super.finished(description);
	}
	
	public void closeAll() {
		closeResultSets();
		closeStatements();
		closeConnections();
        currentConnection = null;
	}

	public ResultSet executeQuery(String sql) throws Exception {
		Statement s = getStatement();
		ResultSet rs = s.executeQuery(sql);
		resultSets.add(rs);
		return rs;
	}
	
	   public Statement getStatement() throws Exception {
			Statement s = getOrCreateConnection().createStatement();
			statements.add(s);
			return s;
		}
	   public CallableStatement prepareCall(String sql) throws Exception {
		   CallableStatement s = getOrCreateConnection().prepareCall(sql);
			statements.add(s);
			return s;
		}
	   public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws Exception {
		   CallableStatement s = getOrCreateConnection().prepareCall(sql, resultSetType, resultSetConcurrency);
			statements.add(s);
			return s;
		}
	   public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws Exception {
		   CallableStatement s = getOrCreateConnection().prepareCall(sql, resultSetType, resultSetConcurrency,resultSetHoldability);
			statements.add(s);
			return s;
		}
	   
	   public void setAutoCommit(boolean autoCommit) throws Exception {
		   getOrCreateConnection().setAutoCommit(autoCommit);
	   }
	   public void rollback() throws Exception {
		   getOrCreateConnection().rollback();
	   }
	   public void commit() throws Exception {
		   getOrCreateConnection().commit();
	   }
	   public void splitTable(String tableName, String schemaName) throws Exception{
			 ConglomerateUtils.splitConglomerate(getConglomId(tableName, schemaName));
	    }

		public long getConglomId(String tableName, String schemaName) throws Exception{
	       /*
	        * This is a needlessly-complicated and annoying way of doing this,
	        * because *when it was written*, the metadata information was kind of all messed up
	        * and doing a join between systables and sysconglomerates resulted in an error. When you are
	        * looking at this code and going WTF?!? feel free to try cleaning up the SQL. If you get a bunch of
	        * wonky errors, then we haven't fixed the underlying issue yet. If you don't, then you just cleaned up
	        * some ugly-ass code. Good luck to you.
	        *
	        */
			ResultSet rs = executeQuery(String.format("select tableid from sys.systables t inner join sys.sysschemas s on t.schemaid = s.schemaid " +
					"where t.tablename = '%s' and s.schemaname = '%s'",tableName.toUpperCase(),schemaName.toUpperCase()));
			if(rs.next()){
				String tableid = rs.getString(1);

				rs.close();
				rs = executeQuery(String.format("select * from sys.sysconglomerates where tableid = '%s'",tableid));
				if(rs.next()){
					return rs.getLong(3);
				}else{
					throw new Exception(String.format("Missing conglomerate to split for table id = %s",tableid));
				}

			}else{
				LOG.warn("Unable to find the conglom id for table  "+tableName);
			}
			return -1l;
		}

		public void splitTable(String tableName, String schemaName, int position) throws Exception{
			Scan scan = new Scan();
			scan.setCaching(100);
			scan.addFamily(HBaseConstants.DEFAULT_FAMILY_BYTES);

			long conglomId = getConglomId(tableName,schemaName);
			HTable table = null;
			ResultScanner scanner = null;
			try {
				table = new HTable(conglomId+"");
				scanner = table.getScanner(scan);
				int count = 0;
				Result result = null;
				while(count < position){
					Result next = scanner.next();
					if(next==null){
						break;
					}else{
						result = next;
					}
					count++;
				}
				if(result!=null)
					ConglomerateUtils.splitConglomerate(conglomId, result.getRow());
			} catch (Exception e) {
				SpliceLogUtils.logAndThrow(LOG, "error splitting table", e);
				throw e;
			} finally {
				Closeables.closeQuietly(scanner);
				Closeables.closeQuietly(table);
			}
		}
}
