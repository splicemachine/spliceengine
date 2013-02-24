package org.apache.derby.impl.sql.execute.operations;

import java.sql.*;

import com.splicemachine.derby.test.SpliceDerbyTest;
import com.splicemachine.derby.test.SpliceNetDerbyTest;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.splicemachine.derby.test.SpliceDerbyTest;

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
        DatabaseMetaData dmd;
        conn.setAutoCommit(false);
        try{
        	conn.setAutoCommit(false);
            dmd = conn.getMetaData();
            resultSet = dmd.getProcedures(null, null, null);
            while(resultSet.next()){
                SpliceLogUtils.info(LOG,"c1=%s,c2=%s,c3=%s",resultSet.getString(1),resultSet.getString(2),resultSet.getString(3));
            }
            conn.commit();
        }finally{
            if(resultSet!=null)resultSet.close();
        }
    }

//    @Ignore
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
    public void testCallSQLTABLES() throws Exception{
        CallableStatement cs = null;
        ResultSet rs = null;
        Statement s = null;
        try{
            //create a table in the APP schema
//            s = conn.createStatement();
//            s.execute("create table test(a int)");
            cs = conn.prepareCall("call SYSIBM.SQLTABLES(null,'SYS',null,'SYSTEM TABLE',null)",ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
            rs = cs.executeQuery();
            int count =0;
            while(rs.next()){
                Object data = rs.getObject(2);
                LOG.info(String.format("schema=%s,table=%s",data,rs.getString(3)));
                count++;
            }
            Assert.assertTrue("Incorrect rows returned!",count>0);
        }finally{
            if(rs!=null)rs.close();
            if(s!=null)s.close();
            if(cs!=null)cs.close();
        }
    }

    @Test
    public void testCastScan() throws Exception{
        PreparedStatement ps = null;
        ResultSet rs = null;
        try{
            ps = conn.prepareStatement("select cast('' as varchar(128)) as cat,schemaname from sys.sysschemas");
            rs = ps.executeQuery();
            int count=0;
            while(rs.next()){
                LOG.info(String.format("%s,%s",rs.getObject(1),rs.getObject(2)));
                count++;
            }
            Assert.assertTrue("incorrect count returned!",count>0);
        }finally{
            if(rs!=null)rs.close();
            if(ps!=null)ps.close();
        }
    }

    @Test
    public void testGetTables() {
    	Statement s = null;
        ResultSet rs = null;
        try{
            s = conn.createStatement();
//            s.execute("create table test(value int)");
//            s.execute("create table test2(value int)");
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
            rs = s.executeQuery("SELECT " +
                    "CAST ('' AS VARCHAR(128)) AS TABLE_CAT, " +
                    "SCHEMANAME AS TABLE_SCHEM,  " +
                    "TABLENAME AS TABLE_NAME, " +
            		"(CAST (RTRIM(TABLE_TYPE) AS VARCHAR(12))) AS TABLE_TYPE, " +
                    "CAST ('' AS VARCHAR(128)) AS REMARKS, " +
            		"CAST (NULL AS VARCHAR(128)) AS TYPE_CAT, " +
                    "CAST (NULL AS VARCHAR(128)) AS TYPE_SCHEM, " +
                    "CAST (NULL AS VARCHAR(128)) AS TYPE_NAME, " +
            		"CAST (NULL AS VARCHAR(128)) AS SELF_REFERENCING_COL_NAME, " +
                    "CAST (NULL AS VARCHAR(128)) AS REF_GENERATION " +
            		"FROM SYS.SYSTABLES, " +
                    "SYS.SYSSCHEMAS, " +
                    "(VALUES ('T','TABLE'), ('S','SYSTEM TABLE'), ('V', 'VIEW'), ('A', 'SYNONYM')) T(TTABBREV,TABLE_TYPE) " +
            		"WHERE " +
                    "(TTABBREV=TABLETYPE 	" +
                    "AND (SYS.SYSTABLES.SCHEMAID = SYS.SYSSCHEMAS.SCHEMAID) " +
                    "AND (SYS.SYSSCHEMAS.SCHEMANAME LIKE '%') " +
            		"AND (TABLENAME LIKE '%')) " +
//                    "AND TABLETYPE IN ('TABLE', 'SYSTEM TABLE', 'VIEW', 'SYNONYM')) " +
            		"ORDER BY " +
                    "TABLE_TYPE, " +
                    "TABLE_SCHEM, " +
                    "TABLE_NAME");
            int count =0;
            while(rs.next()){
                LOG.info(String.format("2=%s",rs.getString(2)));
                count++;
            }
            Assert.assertTrue("incorrect rows returned",count>0);
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
