package org.apache.derby.impl.sql.execute.operations;

import java.sql.CallableStatement;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.commons.dbutils.DbUtils;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

public class CallStatementOperationTest extends SpliceUnitTest {
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	public static final String CLASS_NAME = CallStatementOperationTest.class.getSimpleName().toUpperCase();
	private static Logger LOG = Logger.getLogger(CallStatementOperationTest.class);
	protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher("TEST1",CLASS_NAME,"(a int)");
	
	@ClassRule 
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher).around(spliceTableWatcher);
	
	@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    public void testCallSqlColumns() throws Exception{
        int count =0;
        DatabaseMetaData dmd = methodWatcher.getOrCreateConnection().getMetaData();
        ResultSet rs = dmd.getColumns(null,"SYS","SYSSCHEMAS",null);
            while(rs.next()){
                count++;
            }
            Assert.assertTrue("No Rows returned!",count>0);
            DbUtils.closeQuietly(rs);
    }

    @Test
    public void testCallIndexInfo() throws Exception {
        	DatabaseMetaData dmd = methodWatcher.getOrCreateConnection().getMetaData();
        	ResultSet rs = dmd.getIndexInfo(null,"SYS","SYSSCHEMAS",false,true);
            while(rs.next()){ // TODO No Test
            }
            DbUtils.closeQuietly(rs);
    }

	@Test
	public void testCallSqlProcedures() throws Exception {
        	DatabaseMetaData dmd = methodWatcher.getOrCreateConnection().getMetaData();
            ResultSet rs = dmd.getProcedures(null, null, null);
            while(rs.next()){ // TODO No Test
            }
            DbUtils.closeQuietly(rs);
    }

    @Test
    public void testConnectionMetadata() throws Exception{
        	DatabaseMetaData dmd = methodWatcher.getOrCreateConnection().getMetaData();
        	ResultSet rs = dmd.getColumns(null,null,"CATEGORY",null);
            while(rs.next()){ // TODO No Test
            }
            DbUtils.closeQuietly(rs);
    }

    @Test
	public void testCallSysSchemas() throws Exception {
    		methodWatcher.getOrCreateConnection().setAutoCommit(true);
			CallableStatement cs = methodWatcher.prepareCall("CALL SYSIBM.METADATA()");			
			ResultSet rs = cs.executeQuery();			
			int count = 0;
			while (rs.next()) {
				Assert.assertTrue(rs.getBoolean(1));
				count++;
			}
			Assert.assertEquals(1, count);
            DbUtils.closeQuietly(rs);
	}

    @Ignore
    public void testCallGetTypeInfo() throws Exception{
            CallableStatement cs = methodWatcher.prepareCall("call SYSIBM.SQLGETTYPEINFO(0,null)");
            ResultSet rs = cs.executeQuery();
            while(rs.next()){ // TODO No test
            }
            DbUtils.closeQuietly(rs);
    }

    @Test
    public void testCallSQLTABLES() throws Exception{
            CallableStatement cs = methodWatcher.prepareCall("call SYSIBM.SQLTABLES(null,'SYS',null,'SYSTEM TABLE',null)",ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
            ResultSet rs = cs.executeQuery();
            int count =0;
            while(rs.next()){
                Object data = rs.getObject(2);
                count++;
            }
            Assert.assertTrue("Incorrect rows returned!",count>0);
            DbUtils.closeQuietly(rs);
    }


	
    @Test
    public void testCallSQLTABLESInAppSchema() throws Exception{
            CallableStatement cs = methodWatcher.prepareCall("call SYSIBM.SQLTABLES(null,'"+CallStatementOperationTest.class.getSimpleName().toUpperCase()+"',null,null,null)",ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
            ResultSet rs = cs.executeQuery();
            int count =0;
            while(rs.next()){
                Object data = rs.getObject(2);
                count++;
            }
            Assert.assertTrue("Incorrect rows returned!",count>0);
            DbUtils.closeQuietly(rs);
    }

    @Test
    public void testCastScan() throws Exception{
            PreparedStatement ps = methodWatcher.prepareStatement("select cast('' as varchar(128)) as cat,schemaname from sys.sysschemas");
            ResultSet rs = ps.executeQuery();
            int count=0;
            while(rs.next()){
                count++;
            }
            Assert.assertTrue("incorrect count returned!",count>0);
    }

    @Test
    public void testGetTables() throws Exception {
           Statement s = methodWatcher.getStatement();
           ResultSet rs = s.executeQuery("SELECT " +
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
                count++;
            }
            Assert.assertTrue("incorrect rows returned",count>0);
        }	
    
}
