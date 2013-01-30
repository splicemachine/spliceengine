package org.apache.derby.impl.sql.execute.operations;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.splicemachine.derby.test.SpliceDerbyTest;

public class CallStatementOperationTest extends SpliceDerbyTest {
	private static Logger LOG = Logger.getLogger(CallStatementOperationTest.class);
	
	@BeforeClass 
	public static void startup() throws SQLException {
		startConnection();	
	} 


	@Test
	public void testCallSysSchemas() throws SQLException {
		LOG.info("start testCallStatement");
		Statement s = null;
		ResultSet result = null;
		try {
			CallableStatement cs = conn.prepareCall("CALL SYSIBM.SQLTABLES('', '', '', '', 'GETSCHEMAS=1')");
			result = cs.executeQuery();
			while (result.next()) {
				LOG.info("TABLE_SCHEM="+result.getString(1));
			}
		} catch (SQLException e) {
			LOG.error("error in testAggregatedDeleteExt-"+e.getMessage(), e);
		} finally {
			try {
				if (result!=null)
					result.close();
				if(s!=null)
					s.close();
			} catch (SQLException e) {
				//no need to print out
			}
		}		
	}
	
	@AfterClass 
	public static void shutdown() throws SQLException {
		stopConnection();		
	}
}
