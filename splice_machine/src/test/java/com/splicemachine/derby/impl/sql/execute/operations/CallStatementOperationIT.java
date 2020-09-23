/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.sql.execute.operations;

import java.sql.CallableStatement;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

import com.splicemachine.test.SlowTest;
import org.apache.commons.dbutils.DbUtils;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import splice.com.google.common.collect.Sets;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;

public class CallStatementOperationIT extends SpliceUnitTest {
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	public static final String CLASS_NAME = CallStatementOperationIT.class.getSimpleName().toUpperCase();
	private static Logger LOG = Logger.getLogger(CallStatementOperationIT.class);
	protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher("TEST1",CLASS_NAME,"(a int)");
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);	

	@ClassRule 
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher).
	around(spliceSchemaWatcher).around(spliceTableWatcher);
	
	@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    public void testCallSqlColumns() throws Exception{
        int count =0;
        DatabaseMetaData dmd = methodWatcher.getOrCreateConnection().getMetaData();
        ResultSet rs = dmd.getColumns(null,"SYS","SYSSCHEMAS",null);
        Set<String> correctNames = Sets.newHashSet("SCHEMAID","SCHEMANAME","AUTHORIZATIONID");
        while(rs.next()){
            String sIdCol = rs.getString(4);
            int colType = rs.getInt(5);
            Assert.assertTrue("No colType returned!",!rs.wasNull());
            int colNum = rs.getInt(17);
            Assert.assertTrue("No colNum returned!",!rs.wasNull());

            Assert.assertTrue("Incorrect column name returned!",correctNames.contains(sIdCol.toUpperCase()));
            count++;
        }
        Assert.assertEquals("incorrect rows returned!",3,count);
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

    @Test
    public void testCallGetTypeInfo() throws Exception{
        String[] expectedTypes = {"BIGINT", "LONG VARCHAR FOR BIT DATA", "VARCHAR () FOR BIT DATA", "CHAR () FOR BIT DATA",
            "LONG VARCHAR", "CHAR", "NUMERIC", "DECIMAL", "INTEGER", "SMALLINT", "FLOAT", "REAL", "DOUBLE", "VARCHAR",
            "BOOLEAN", "DATE", "TIME", "TIMESTAMP", "OBJECT", "BLOB", "CLOB", "XML"};
        Arrays.sort(expectedTypes);

        CallableStatement cs = methodWatcher.prepareCall("call SYSIBM.SQLGETTYPEINFO(0,null)");
        ResultSet rs = cs.executeQuery();
        try {
            List<String> actual = new ArrayList<>(expectedTypes.length);
            while (rs.next()) {
                actual.add(rs.getString(1));
            }
            String[] actualArray = actual.toArray(new String[actual.size()]);
            Arrays.sort(actualArray);
            Assert.assertArrayEquals(expectedTypes, actualArray);
        } finally {
            DbUtils.closeQuietly(rs);
        }
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
    @Category(SlowTest.class)
    public void testCallSQLTABLESInAppSchema() throws Exception{
            CallableStatement cs = methodWatcher.prepareCall("call SYSIBM.SQLTABLES(null,'"+CallStatementOperationIT.class.getSimpleName().toUpperCase()+"',null,null,null)",ResultSet.TYPE_FORWARD_ONLY,ResultSet.CONCUR_READ_ONLY);
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
    @Category(SlowTest.class)
    public void testRepeatedCallSQLTABLESInAppSchema() throws Exception {
       for(int i=0;i<10;i++){
           testCallSQLTABLESInAppSchema();
       }
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

    @Test
    public void testSysGetTableTypes() throws Exception {
        // Was throwing NPE, just test that doesn't blow
        try {
        Assert.assertTrue(methodWatcher.prepareCall("EXECUTE STATEMENT SYS.\"getTableTypes\"").execute());
        } catch (Exception e) {
            Assert.fail("Blowing on getTableTypes");
        }
    }
}
