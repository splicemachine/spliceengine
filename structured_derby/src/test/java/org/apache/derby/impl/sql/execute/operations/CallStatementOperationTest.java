package org.apache.derby.impl.sql.execute.operations;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.splicemachine.derby.test.SpliceNetDerbyTest;

//public class CallStatementOperationTest extends SpliceDerbyTest {
public class CallStatementOperationTest extends SpliceNetDerbyTest {
	private static Logger LOG = Logger.getLogger(CallStatementOperationTest.class);
	
	@BeforeClass 
	public static void startup() throws Exception {
		startConnection();	
	} 


	@Test
	public void testCallSqlProcedures() throws SQLException {
        ResultSet resultSet = null;
        conn.setAutoCommit(false);
        try{
        	conn.setAutoCommit(false);
            resultSet = conn.getMetaData().getProcedures(null, null, null);
            while(resultSet.next()){
                LOG.info("c1="+resultSet.getString(1));
            }
            conn.commit();
        }finally{
            if(resultSet!=null)resultSet.close();
        }
    }

    @Ignore
	public void testCallSysSchemas() throws SQLException {
    	conn.setAutoCommit(true);
		LOG.info("start testCallStatement");
		Statement s = null;
		ResultSet result = null;
		try {
			//CallableStatement cs = conn.prepareCall("CALL SYSIBM.SQLTABLES('', '', '', '', 'GETSCHEMAS=1')");
			CallableStatement cs = conn.prepareCall("CALL SYSIBM.METADATA()");
			
			//CallableStatement cs = conn.prepareCall("CALL SYSIBM.SQLGETTYPEINFO(0,null)");
			result = cs.executeQuery();
			
			//s = conn.createStatement();
			//result = s.executeQuery("SELECT SCHEMANAME AS TABLE_SCHEM, CAST(NULL AS VARCHAR(128)) AS TABLE_CATALOG FROM SYS.SYSSCHEMAS WHERE SCHEMANAME LIKE '%' ORDER BY TABLE_SCHEM");
			
			int count = 0;
			while (result.next()) {
				LOG.info("c1="+result.getString(1)+",c2="+result.getString(2));
				Assert.assertTrue(result.getBoolean(1));
				count++;
			}
			Assert.assertEquals(11, count);
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

    @Ignore
    public void testCallGetTypeInfo() throws Exception{
        Statement s = null;
        ResultSet rs = null;
        try{
            CallableStatement cs = conn.prepareCall("call SYSIBM.SQLGETTYPEINFO(0,null)");
            rs = cs.executeQuery();
            while(rs.next()){
                LOG.info(String.format("1=%s",rs.getString(1)));
            }
        }finally{
            if(rs!=null)rs.close();
            if(s!=null)s.close();
        }
    }

    @Test
    public void testGetTables() {
    	Statement s = null;
        ResultSet rs = null;
        try{
            s = conn.createStatement();
            
            /* Original statement
             * 
             * SELECT CAST ('' AS VARCHAR(128)) AS TABLE_CAT, SCHEMANAME AS TABLE_SCHEM,  TABLENAME AS TABLE_NAME, 
             * (CAST (RTRIM(TABLE_TYPE) AS VARCHAR(12))) AS TABLE_TYPE, CAST ('' AS VARCHAR(128)) AS REMARKS, CAST (NULL AS VARCHAR(128)) AS TYPE_CAT, 
             * CAST (NULL AS VARCHAR(128)) AS TYPE_SCHEM, CAST (NULL AS VARCHAR(128)) AS TYPE_NAME, CAST (NULL AS VARCHAR(128)) AS SELF_REFERENCING_COL_NAME, 
             * CAST (NULL AS VARCHAR(128)) AS REF_GENERATION FROM SYS.SYSTABLES, SYS.SYSSCHEMAS, (VALUES ('T','TABLE'), ('S','SYSTEM TABLE'), ('V', 'VIEW'), 
             * ('A', 'SYNONYM')) T(TTABBREV,TABLE_TYPE) WHERE (TTABBREV=TABLETYPE 	AND (SYS.SYSTABLES.SCHEMAID = SYS.SYSSCHEMAS.SCHEMAID) 
             * AND ((1=1) OR ? IS NOT NULL) AND (SYS.SYSSCHEMAS.SCHEMANAME LIKE ?) AND (TABLENAME LIKE ?) AND TABLETYPE IN (?, ?, ?, ?)) 
             * ORDER BY TABLE_TYPE, TABLE_SCHEM, TABLE_NAME
             * 
             */
            rs = s.executeQuery("SELECT CAST ('' AS VARCHAR(128)) AS TABLE_CAT, SCHEMANAME AS TABLE_SCHEM,  TABLENAME AS TABLE_NAME, " +          
            		"(CAST (RTRIM(TABLE_TYPE) AS VARCHAR(12))) AS TABLE_TYPE, CAST ('' AS VARCHAR(128)) AS REMARKS, " +
            		"CAST (NULL AS VARCHAR(128)) AS TYPE_CAT, CAST (NULL AS VARCHAR(128)) AS TYPE_SCHEM, CAST (NULL AS VARCHAR(128)) AS TYPE_NAME, " +
            		"CAST (NULL AS VARCHAR(128)) AS SELF_REFERENCING_COL_NAME, CAST (NULL AS VARCHAR(128)) AS REF_GENERATION FROM SYS.SYSTABLES, " +
            		"SYS.SYSSCHEMAS, (VALUES ('T','TABLE'), ('S','SYSTEM TABLE'), ('V', 'VIEW'), ('A', 'SYNONYM')) T(TTABBREV,TABLE_TYPE) " +
            		"WHERE (TTABBREV=TABLETYPE 	AND (SYS.SYSTABLES.SCHEMAID = SYS.SYSSCHEMAS.SCHEMAID)  " +
            		"AND (SYS.SYSSCHEMAS.SCHEMANAME LIKE '%APP%') " +
            		"AND (TABLENAME LIKE '%') AND TABLETYPE like '%') ORDER BY TABLE_TYPE, TABLE_SCHEM, TABLE_NAME");
            while(rs.next()){
                LOG.info(String.format("1=%s",rs.getString(1)));
            }
        }catch (Exception e) {
        	e.printStackTrace();
        } finally{
        	try {
            if(rs!=null)rs.close();
            if(s!=null)s.close();
        	} catch (Exception e) {
        		
        	}
        }
        
    	
    }
    
	@AfterClass 
	public static void shutdown() throws SQLException {
		stopConnection();		
	}
}
