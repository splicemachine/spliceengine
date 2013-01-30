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
	/* The same code runs in plain Derby, and produces the following results, but in our system, nothing got printed out.
	 * 	TABLE_SCHEM=APP
	 	TABLE_SCHEM=NULLID
		TABLE_SCHEM=SQLJ
		TABLE_SCHEM=SYS
		TABLE_SCHEM=SYSCAT
		TABLE_SCHEM=SYSCS_DIAG
		TABLE_SCHEM=SYSCS_UTIL
		TABLE_SCHEM=SYSFUN
		TABLE_SCHEM=SYSIBM
		TABLE_SCHEM=SYSPROC
		TABLE_SCHEM=SYSSTAT
	 */
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
