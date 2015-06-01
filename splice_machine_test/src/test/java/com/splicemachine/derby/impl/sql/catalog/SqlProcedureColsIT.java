package com.splicemachine.derby.impl.sql.catalog;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;

/**
 * Tests for the SYSIBM.SQLPROCEDURECOLS stored procedure.
 * This stored procedure is used by the DatabaseMetaData.getProcedureColumns() API in JDBC and ODBC.
 *
 * @author David Winters
 *		 Created on: 9/25/14
 */
public class SqlProcedureColsIT extends SpliceUnitTest {
	public static final String CLASS_NAME = SqlProcedureColsIT.class.getSimpleName().toUpperCase();

	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

	@ClassRule
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
			.around(spliceSchemaWatcher);

	@Rule
	public SpliceWatcher methodWatcher = new SpliceWatcher();

	@Test
	public void testCatalogNameLikeComparison() throws Exception {
		ResultSet rs = null;
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLPROCEDURECOLS(null, null, 'SQLPROCEDURECOLS', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 5, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLPROCEDURECOLS('%', null, 'SQLPROCEDURECOLS', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 5, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLPROCEDURECOLS('%IBM', null, 'SQLPROCEDURECOLS', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 5, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLPROCEDURECOLS('SYS%', null, 'SQLPROCEDURECOLS', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 5, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLPROCEDURECOLS('SYSIBM', null, 'SQLPROCEDURECOLS', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 5, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLPROCEDURECOLS('SY%BM', null, 'SQLPROCEDURECOLS', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 5, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLPROCEDURECOLS('S%S%BM', null, 'SQLPROCEDURECOLS', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 5, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLPROCEDURECOLS('S%S%B%', null, 'SQLPROCEDURECOLS', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 5, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLPROCEDURECOLS('%Y%%B%', null, 'SQLPROCEDURECOLS', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 5, resultSetSize(rs));
	}

	@Test
	public void testSchemaNameLikeComparison() throws Exception {
		ResultSet rs = null;
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLPROCEDURECOLS(null, null, 'SQLPROCEDURECOLS', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 5, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLPROCEDURECOLS(null, '%', 'SQLPROCEDURECOLS', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 5, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLPROCEDURECOLS(null, '%IBM', 'SQLPROCEDURECOLS', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 5, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLPROCEDURECOLS(null, 'SYS%', 'SQLPROCEDURECOLS', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 5, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLPROCEDURECOLS(null, 'SYSIBM', 'SQLPROCEDURECOLS', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 5, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLPROCEDURECOLS(null, 'SY%BM', 'SQLPROCEDURECOLS', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 5, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLPROCEDURECOLS(null, 'S%S%BM', 'SQLPROCEDURECOLS', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 5, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLPROCEDURECOLS(null, 'S%S%B%', 'SQLPROCEDURECOLS', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 5, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLPROCEDURECOLS(null, '%Y%%B%', 'SQLPROCEDURECOLS', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 5, resultSetSize(rs));
	}

	@Test
	public void testProcNameLikeComparison() throws Exception {
		ResultSet rs = null;
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLPROCEDURECOLS(null, null, null, null, null)");
		Assert.assertTrue("Incorrect rows returned!", resultSetSize(rs) > 100);
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLPROCEDURECOLS(null, null, '%', null, null)");
		Assert.assertTrue("Incorrect rows returned!", resultSetSize(rs) > 100);
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLPROCEDURECOLS(null, null, '%PROCEDURECOLS', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 5, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLPROCEDURECOLS(null, null, 'SQLPROCEDURE%', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 9, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLPROCEDURECOLS(null, null, 'SQLPROCEDURECOLS', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 5, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLPROCEDURECOLS(null, null, 'SQLPROC%DURECOLS', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 5, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLPROCEDURECOLS(null, null, 'SQLPROC%DUR%COLS', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 5, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLPROCEDURECOLS(null, null, 'SQLPR%CEDUREC%L%', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 5, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLPROCEDURECOLS(null, null, '%QLPR%C%D%R%C%L%', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 5, resultSetSize(rs));
	}

	@Test
	public void testParamNameLikeComparison() throws Exception {
		ResultSet rs = null;
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLPROCEDURECOLS(null, 'SYSIBM', 'SQLPROCEDURECOLS', null, null)");
		Assert.assertEquals("Incorrect rows returned!", 5, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLPROCEDURECOLS(null, 'SYSIBM', 'SQLPROCEDURECOLS', '%', null)");
		Assert.assertEquals("Incorrect rows returned!", 5, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLPROCEDURECOLS(null, 'SYSIBM', 'SQLPROCEDURECOLS', '%NAME', null)");
		Assert.assertEquals("Incorrect rows returned!", 4, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLPROCEDURECOLS(null, 'SYSIBM', 'SQLPROCEDURECOLS', 'PROC%', null)");
		Assert.assertEquals("Incorrect rows returned!", 1, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLPROCEDURECOLS(null, 'SYSIBM', 'SQLPROCEDURECOLS', 'PROCNAME', null)");
		Assert.assertEquals("Incorrect rows returned!", 1, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLPROCEDURECOLS(null, 'SYSIBM', 'SQLPROCEDURECOLS', 'PRO%AME', null)");
		Assert.assertEquals("Incorrect rows returned!", 1, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLPROCEDURECOLS(null, 'SYSIBM', 'SQLPROCEDURECOLS', 'PR%CN%ME', null)");
		Assert.assertEquals("Incorrect rows returned!", 1, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLPROCEDURECOLS(null, 'SYSIBM', 'SQLPROCEDURECOLS', 'PR%CN%M%', null)");
		Assert.assertEquals("Incorrect rows returned!", 1, resultSetSize(rs));
		rs = methodWatcher.executeQuery("CALL SYSIBM.SQLPROCEDURECOLS(null, 'SYSIBM', 'SQLPROCEDURECOLS', '%R%CN%M%', null)");
		Assert.assertEquals("Incorrect rows returned!", 1, resultSetSize(rs));
	}

	@Test
	public void testResultSetColumnTypesForJDBC() throws Exception {
		ResultSet rs = methodWatcher.executeQuery("CALL SYSIBM.SQLPROCEDURECOLS(null, 'SYSIBM', 'SQLPROCEDURECOLS', 'PROCNAME', null)");
		int count = 0;
		while (rs.next()) {
			ResultSetMetaData rsmd = rs.getMetaData();
			Assert.assertEquals("Incorrect SQL type for column!", Types.VARCHAR, rsmd.getColumnType(1));
			Assert.assertEquals("Incorrect SQL type for column!", Types.VARCHAR, rsmd.getColumnType(2));
			Assert.assertEquals("Incorrect SQL type for column!", Types.VARCHAR, rsmd.getColumnType(3));
			Assert.assertEquals("Incorrect SQL type for column!", Types.VARCHAR, rsmd.getColumnType(4));
			Assert.assertEquals("Incorrect SQL type for column!", Types.SMALLINT, rsmd.getColumnType(5));
			Assert.assertEquals("Incorrect SQL type for column!", Types.INTEGER, rsmd.getColumnType(6));
			Assert.assertEquals("Incorrect SQL type for column!", Types.VARCHAR, rsmd.getColumnType(7));
			Assert.assertEquals("Incorrect SQL type for column!", Types.INTEGER, rsmd.getColumnType(8));
			Assert.assertEquals("Incorrect SQL type for column!", Types.INTEGER, rsmd.getColumnType(9));
			Assert.assertEquals("Incorrect SQL type for column!", Types.SMALLINT, rsmd.getColumnType(10));
			Assert.assertEquals("Incorrect SQL type for column!", Types.SMALLINT, rsmd.getColumnType(11));
			Assert.assertEquals("Incorrect SQL type for column!", Types.SMALLINT, rsmd.getColumnType(12));
			Assert.assertEquals("Incorrect SQL type for column!", Types.VARCHAR, rsmd.getColumnType(13));
			Assert.assertEquals("Incorrect SQL type for column!", Types.VARCHAR, rsmd.getColumnType(14));
			Assert.assertEquals("Incorrect SQL type for column!", Types.INTEGER, rsmd.getColumnType(15));
			Assert.assertEquals("Incorrect SQL type for column!", Types.INTEGER, rsmd.getColumnType(16));
			Assert.assertEquals("Incorrect SQL type for column!", Types.INTEGER, rsmd.getColumnType(17));
			Assert.assertEquals("Incorrect SQL type for column!", Types.INTEGER, rsmd.getColumnType(18));
			Assert.assertEquals("Incorrect SQL type for column!", Types.VARCHAR, rsmd.getColumnType(19));
			Assert.assertEquals("Incorrect SQL type for column!", Types.VARCHAR, rsmd.getColumnType(20));
			Assert.assertEquals("Incorrect SQL type for column!", Types.SMALLINT, rsmd.getColumnType(21));
			Assert.assertEquals("Incorrect SQL type for column!", Types.SMALLINT, rsmd.getColumnType(22));
			count++;
		}
		Assert.assertEquals("Incorrect rows returned!", 1, count);
	}

	@Test
	public void testResultSetColumnTypesForODBC() throws Exception {
		ResultSet rs = methodWatcher.executeQuery("CALL SYSIBM.SQLPROCEDURECOLS(null, 'SYSIBM', 'SQLPROCEDURECOLS', 'PROCNAME', 'DATATYPE=''ODBC''')");
		int count = 0;
		while (rs.next()) {
			ResultSetMetaData rsmd = rs.getMetaData();
			Assert.assertEquals("Incorrect SQL type for column!", Types.VARCHAR, rsmd.getColumnType(1));
			Assert.assertEquals("Incorrect SQL type for column!", Types.VARCHAR, rsmd.getColumnType(2));
			Assert.assertEquals("Incorrect SQL type for column!", Types.VARCHAR, rsmd.getColumnType(3));
			Assert.assertEquals("Incorrect SQL type for column!", Types.VARCHAR, rsmd.getColumnType(4));
			Assert.assertEquals("Incorrect SQL type for column!", Types.SMALLINT, rsmd.getColumnType(5));
			Assert.assertEquals("Incorrect SQL type for column!", Types.SMALLINT, rsmd.getColumnType(6));
			Assert.assertEquals("Incorrect SQL type for column!", Types.VARCHAR, rsmd.getColumnType(7));
			Assert.assertEquals("Incorrect SQL type for column!", Types.INTEGER, rsmd.getColumnType(8));
			Assert.assertEquals("Incorrect SQL type for column!", Types.INTEGER, rsmd.getColumnType(9));
			Assert.assertEquals("Incorrect SQL type for column!", Types.SMALLINT, rsmd.getColumnType(10));
			Assert.assertEquals("Incorrect SQL type for column!", Types.SMALLINT, rsmd.getColumnType(11));
			Assert.assertEquals("Incorrect SQL type for column!", Types.SMALLINT, rsmd.getColumnType(12));
			Assert.assertEquals("Incorrect SQL type for column!", Types.VARCHAR, rsmd.getColumnType(13));
			Assert.assertEquals("Incorrect SQL type for column!", Types.VARCHAR, rsmd.getColumnType(14));
			Assert.assertEquals("Incorrect SQL type for column!", Types.SMALLINT, rsmd.getColumnType(15));
			Assert.assertEquals("Incorrect SQL type for column!", Types.SMALLINT, rsmd.getColumnType(16));
			Assert.assertEquals("Incorrect SQL type for column!", Types.INTEGER, rsmd.getColumnType(17));
			Assert.assertEquals("Incorrect SQL type for column!", Types.INTEGER, rsmd.getColumnType(18));
			Assert.assertEquals("Incorrect SQL type for column!", Types.VARCHAR, rsmd.getColumnType(19));
			count++;
		}
		Assert.assertEquals("Incorrect rows returned!", 1, count);
	}

    @Test
    public void testSYSIBMSQLUDTS() throws Exception {
        ResultSet rs = null;
        rs = methodWatcher.executeQuery("CALL SYSIBM.SQLUDTS(null,null,null,null,null)");
        Assert.assertEquals("Incorrect rows returned!", 0, resultSetSize(rs));
    }

    @Test
    public void testSYSIBMSQLTABLES() throws Exception {
        ResultSet rs = null;
        rs = methodWatcher.executeQuery("CALL SYSIBM.SQLTABLES(null,null,null,null,null)");
        Assert.assertNotEquals("Incorrect rows returned!", 0, resultSetSize(rs));
    }

    @Test
    public void testSYSIBMSQLTABLEPRIVILEGES() throws Exception {
        ResultSet rs = null;
        rs = methodWatcher.executeQuery("CALL SYSIBM.SQLTABLEPRIVILEGES(null,null,null,null)");
    }

    @Test
    public void testSYSIBMSQLSTATISTICS() throws Exception {
        ResultSet rs = null;
        rs = methodWatcher.executeQuery("CALL SYSIBM.SQLSTATISTICS(null,null,null,0,1,null)");
    }

    @Test
    public void testSYSIBMSQLSPECIALCOLUMNS() throws Exception {
        ResultSet rs = null;
        rs = methodWatcher.executeQuery("CALL SYSIBM.SQLSPECIALCOLUMNS(1,null,null,'FOO',0,0,null)");
    }

    @Test
    public void testSQLPROCEDURES() throws Exception {
        ResultSet rs = null;
        rs = methodWatcher.executeQuery("CALL SYSIBM.SQLPROCEDURES(null,null,null,null)");
    }

    @Test
    public void testSQLPROCEDURECOLS() throws Exception {
        ResultSet rs = null;
        rs = methodWatcher.executeQuery("CALL SYSIBM.SQLPROCEDURECOLS(null,null,null,null,null)");
    }

    @Test
    public void testSQLPRIMARYKEYS() throws Exception {
        ResultSet rs = null;
        rs = methodWatcher.executeQuery("CALL SYSIBM.SQLPRIMARYKEYS(null,null,'FOO',null)");
    }

    @Test
    public void testSQLGETTYPEINFO() throws Exception {
        ResultSet rs = null;
        rs = methodWatcher.executeQuery("CALL SYSIBM.SQLGETTYPEINFO(0,null)");
    }

    @Test
    public void testSQLFUNCTIONS() throws Exception {
        ResultSet rs = null;
        rs = methodWatcher.executeQuery("CALL SYSIBM.SQLFUNCTIONS(null,null,null,null)");
    }


    @Test
    public void testSQLFUNCTIONPARAMS() throws Exception {
        ResultSet rs = null;
        rs = methodWatcher.executeQuery("CALL SYSIBM.SQLFUNCTIONPARAMS(null,null,null,null,null)");
    }
    @Test
    public void testSQLFOREIGNKEYS() throws Exception {
        ResultSet rs = null;
        rs = methodWatcher.executeQuery("CALL SYSIBM.SQLFOREIGNKEYS(null,null,'FOO',null,null,'FOO2',null)");
    }

    @Test
    public void testSQLCOLUMNS() throws Exception {
        ResultSet rs = null;
        rs = methodWatcher.executeQuery("CALL SYSIBM.SQLCOLUMNS(null,null,null,null,null)");
    }



    @Test
    public void testSQLCOLPRIVILEGES() throws Exception {
        ResultSet rs = null;
        rs = methodWatcher.executeQuery("CALL SYSIBM.SQLCOLPRIVILEGES(null,null,'FOO',null,null)");
    }
    /*
    @Test
    public void testSQLCAMESSAGE() throws Exception {
        ResultSet rs = null;
        rs = methodWatcher.executeQuery("CALL SYSIBM.SQLCAMESSAGE(1,null,null,'FOO',0,0,null)");
    }
*/

    @Test
    public void testMETADATA() throws Exception {
        ResultSet rs = null;
        rs = methodWatcher.executeQuery("CALL SYSIBM.METADATA()");
    }
    /*
    @Test
    public void testCLOBTRUNCATE() throws Exception {
        ResultSet rs = null;
        rs = methodWatcher.executeQuery("CALL SYSIBM.CLOBTRUNCATE(1,null,null,'FOO',0,0,null)");
    }

    @Test
    public void testCLOBSETSTRING() throws Exception {
        ResultSet rs = null;
        rs = methodWatcher.executeQuery("CALL SYSIBM.CLOBSETSTRING(1,null,null,'FOO',0,0,null)");
    }

    @Test
    public void testCLOBRELEASELOCATOR() throws Exception {
        ResultSet rs = null;
        rs = methodWatcher.executeQuery("CALL SYSIBM.CLOBRELEASELOCATOR(1,null,null,'FOO',0,0,null)");
    }

    @Test
    public void testBLOBTRUNCATE() throws Exception {
        ResultSet rs = null;
        rs = methodWatcher.executeQuery("CALL SYSIBM.BLOBTRUNCATE(1,null,null,'FOO',0,0,null)");
    }

    @Test
    public void testBLOBSETBYTES() throws Exception {
        ResultSet rs = null;
        rs = methodWatcher.executeQuery("CALL SYSIBM.BLOBSETBYTES(1,null,null,'FOO',0,0,null)");
    }

    @Test
    public void testBLOBRELEASELOCATOR() throws Exception {
        ResultSet rs = null;
        rs = methodWatcher.executeQuery("CALL SYSIBM.BLOBRELEASELOCATOR(1,null,null,'FOO',0,0,null)");
    }
*/
}
