/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.sql.catalog;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;

import com.splicemachine.derby.test.framework.TestConnection;
import org.junit.*;
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

    private TestConnection conn;

    @Before
    public void setUp() throws Exception{
        conn = methodWatcher.getOrCreateConnection();
    }

    @Test
	public void testCatalogNameLikeComparison() throws Exception {
        testRowSize("null","null");
        testRowSize("'%'","null");
        testRowSize("'%IBM'","null");
        testRowSize("'SYS%'","null");
        testRowSize("'SYSIBM'","null");
        testRowSize("'SY%BM'","null");
        testRowSize("'S%S%BM'","null");
        testRowSize("'S%S%B%'","null");
        testRowSize("'%Y%%B%'","null");
	}

    private void testRowSize(String catalogPattern,String schemaPattern) throws Exception{
        Assert.assertEquals("Incorrect rows returned",5,rowSize(catalogPattern, schemaPattern,"'SQLPROCEDURECOLS'"));
    }

    private static final String baseSqlProcPattern = "CALL SYSIBM.SQLPROCEDURECOLS(<CAT>,<SCHEMA>,<TABLE>,<PARAM>,null)";

    private long rowSize(String catalogPattern,String schemaPattern,String tableNamePattern) throws Exception{
        return rowSize(catalogPattern,schemaPattern,tableNamePattern,"null");
    }

    private long rowSize(String catalogPattern,String schemaPattern,String tableNamePattern,String paramPattern) throws Exception{
        String sql = baseSqlProcPattern.replace("<CAT>",catalogPattern).replace("<SCHEMA>",schemaPattern)
                .replace("<TABLE>",tableNamePattern)
                .replace("<PARAM>",paramPattern);
        try(Statement s = conn.createStatement()){
            try(ResultSet rs = s.executeQuery(sql)){
                return resultSetSize(rs);
            }
        }
    }

	@Test
	public void testSchemaNameLikeComparison() throws Exception {
        testRowSize("null","null");
        testRowSize("null","'%'");
        testRowSize("null","'%IBM'");
        testRowSize("null","'SYS%'");
        testRowSize("null","'SYSIBM'");
        testRowSize("null","'SY%BM'");
        testRowSize("null","'S%S%BM'");
        testRowSize("null","'S%S%B%'");
        testRowSize("null","'%Y%%B%'");
	}

	@Test
	public void testProcNameLikeComparison() throws Exception {
		Assert.assertTrue("Incorrect rows returned!", rowSize("null","null","null")> 100);
		Assert.assertTrue("Incorrect rows returned!", rowSize("null","null","'%'") > 100);
		Assert.assertEquals("Incorrect rows returned!", 5, rowSize("null","null","'%PROCEDURECOLS'"));
		Assert.assertEquals("Incorrect rows returned!", 9, rowSize("null","null","'SQLPROCEDURE%'"));
		Assert.assertEquals("Incorrect rows returned!", 5, rowSize("null","null","'SQLPROCEDURECOLS'"));
		Assert.assertEquals("Incorrect rows returned!", 5, rowSize("null","null","'SQLPROC%DURECOLS'"));
		Assert.assertEquals("Incorrect rows returned!", 5, rowSize("null","null","'SQLPROC%DUR%COLS'"));
		Assert.assertEquals("Incorrect rows returned!", 5, rowSize("null","null","'SQLPR%CEDUREC%L%'"));
		Assert.assertEquals("Incorrect rows returned!", 5, rowSize("null","null","'%QLPR%C%D%R%C%L%'"));
	}

	@Test
	public void testParamNameLikeComparison() throws Exception {
		Assert.assertEquals("Incorrect rows returned!", 5, rowSize("null","'SYSIBM'","'SQLPROCEDURECOLS'","null"));
		Assert.assertEquals("Incorrect rows returned!", 5, rowSize("null","'SYSIBM'","'SQLPROCEDURECOLS'","'%'"));
		Assert.assertEquals("Incorrect rows returned!", 4, rowSize("null","'SYSIBM'","'SQLPROCEDURECOLS'","'%NAME'"));
		Assert.assertEquals("Incorrect rows returned!", 1, rowSize("null","'SYSIBM'","'SQLPROCEDURECOLS'","'PROC%'"));
		Assert.assertEquals("Incorrect rows returned!", 1, rowSize("null","'SYSIBM'","'SQLPROCEDURECOLS'","'PROCNAME'"));
		Assert.assertEquals("Incorrect rows returned!", 1, rowSize("null","'SYSIBM'","'SQLPROCEDURECOLS'","'PROC%AME'"));
		Assert.assertEquals("Incorrect rows returned!", 1, rowSize("null","'SYSIBM'","'SQLPROCEDURECOLS'","'PR%CN%ME'"));
		Assert.assertEquals("Incorrect rows returned!", 1, rowSize("null","'SYSIBM'","'SQLPROCEDURECOLS'","'PR%CN%M%'"));
		Assert.assertEquals("Incorrect rows returned!", 1, rowSize("null","'SYSIBM'","'SQLPROCEDURECOLS'","'%R%CN%M%'"));
	}

	@Test
	public void testResultSetColumnTypesForJDBC() throws Exception {
        try(Statement s = conn.createStatement()){
            try(ResultSet rs=s.executeQuery("CALL SYSIBM.SQLPROCEDURECOLS(null, 'SYSIBM', 'SQLPROCEDURECOLS', 'PROCNAME', null)")){
                Assert.assertTrue("No rows returned!",rs.next());
                ResultSetMetaData rsmd=rs.getMetaData();
                Assert.assertEquals("Incorrect SQL type for column [1]!",Types.VARCHAR,rsmd.getColumnType(1));
                Assert.assertEquals("Incorrect SQL type for column [2]!",Types.VARCHAR,rsmd.getColumnType(2));
                Assert.assertEquals("Incorrect SQL type for column [3]!",Types.VARCHAR,rsmd.getColumnType(3));
                Assert.assertEquals("Incorrect SQL type for column [4]!",Types.VARCHAR,rsmd.getColumnType(4));
                Assert.assertEquals("Incorrect SQL type for column [5]!",Types.SMALLINT,rsmd.getColumnType(5));
                Assert.assertEquals("Incorrect SQL type for column [6]!",Types.INTEGER,rsmd.getColumnType(6));
                Assert.assertEquals("Incorrect SQL type for column [7]!",Types.VARCHAR,rsmd.getColumnType(7));
                Assert.assertEquals("Incorrect SQL type for column [8]!",Types.INTEGER,rsmd.getColumnType(8));
                Assert.assertEquals("Incorrect SQL type for column [9]!",Types.INTEGER,rsmd.getColumnType(9));
                Assert.assertEquals("Incorrect SQL type for column [10]!",Types.SMALLINT,rsmd.getColumnType(10));
                Assert.assertEquals("Incorrect SQL type for column [11]!",Types.SMALLINT,rsmd.getColumnType(11));
                Assert.assertEquals("Incorrect SQL type for column [12]!",Types.SMALLINT,rsmd.getColumnType(12));
                Assert.assertEquals("Incorrect SQL type for column [13]!",Types.VARCHAR,rsmd.getColumnType(13));
                Assert.assertEquals("Incorrect SQL type for column [14]!",Types.VARCHAR,rsmd.getColumnType(14));
                Assert.assertEquals("Incorrect SQL type for column [15]!",Types.INTEGER,rsmd.getColumnType(15));
                Assert.assertEquals("Incorrect SQL type for column [16]!",Types.INTEGER,rsmd.getColumnType(16));
                Assert.assertEquals("Incorrect SQL type for column [17]!",Types.INTEGER,rsmd.getColumnType(17));
                Assert.assertEquals("Incorrect SQL type for column [18]!",Types.INTEGER,rsmd.getColumnType(18));
                Assert.assertEquals("Incorrect SQL type for column [19]!",Types.VARCHAR,rsmd.getColumnType(19));
                Assert.assertEquals("Incorrect SQL type for column [20]!",Types.VARCHAR,rsmd.getColumnType(20));
                Assert.assertEquals("Incorrect SQL type for column [21]!",Types.SMALLINT,rsmd.getColumnType(21));
                Assert.assertEquals("Incorrect SQL type for column [22]!",Types.SMALLINT,rsmd.getColumnType(22));

                Assert.assertFalse("Too many rows returned!",rs.next());
            }
        }
	}

	@Test
	public void testResultSetColumnTypesForODBC() throws Exception {
        try(Statement s = conn.createStatement()){
            try(ResultSet rs=s.executeQuery("CALL SYSIBM.SQLPROCEDURECOLS(null, 'SYSIBM', 'SQLPROCEDURECOLS', 'PROCNAME', 'DATATYPE=''ODBC''')")){
                Assert.assertTrue("No rows returned!",rs.next());

                ResultSetMetaData rsmd=rs.getMetaData();
                Assert.assertEquals("Incorrect SQL type for column [1]!",Types.VARCHAR,rsmd.getColumnType(1));
                Assert.assertEquals("Incorrect SQL type for column [2]!",Types.VARCHAR,rsmd.getColumnType(2));
                Assert.assertEquals("Incorrect SQL type for column [3]!",Types.VARCHAR,rsmd.getColumnType(3));
                Assert.assertEquals("Incorrect SQL type for column [4]!",Types.VARCHAR,rsmd.getColumnType(4));
                Assert.assertEquals("Incorrect SQL type for column [5]!",Types.SMALLINT,rsmd.getColumnType(5));
                Assert.assertEquals("Incorrect SQL type for column [6]!",Types.SMALLINT,rsmd.getColumnType(6));
                Assert.assertEquals("Incorrect SQL type for column [7]!",Types.VARCHAR,rsmd.getColumnType(7));
                Assert.assertEquals("Incorrect SQL type for column [8]!",Types.INTEGER,rsmd.getColumnType(8));
                Assert.assertEquals("Incorrect SQL type for column [9]!",Types.INTEGER,rsmd.getColumnType(9));
                Assert.assertEquals("Incorrect SQL type for column [10]!",Types.SMALLINT,rsmd.getColumnType(10));
                Assert.assertEquals("Incorrect SQL type for column [11]!",Types.SMALLINT,rsmd.getColumnType(11));
                Assert.assertEquals("Incorrect SQL type for column [12]!",Types.SMALLINT,rsmd.getColumnType(12));
                Assert.assertEquals("Incorrect SQL type for column [13]!",Types.VARCHAR,rsmd.getColumnType(13));
                Assert.assertEquals("Incorrect SQL type for column [14]!",Types.VARCHAR,rsmd.getColumnType(14));
                Assert.assertEquals("Incorrect SQL type for column [15]!",Types.SMALLINT,rsmd.getColumnType(15));
                Assert.assertEquals("Incorrect SQL type for column [16]!",Types.SMALLINT,rsmd.getColumnType(16));
                Assert.assertEquals("Incorrect SQL type for column [17]!",Types.INTEGER,rsmd.getColumnType(17));
                Assert.assertEquals("Incorrect SQL type for column [18]!",Types.INTEGER,rsmd.getColumnType(18));
                Assert.assertEquals("Incorrect SQL type for column [19]!",Types.VARCHAR,rsmd.getColumnType(19));

                Assert.assertFalse("Too many rows returned!",rs.next());
            }
        }
	}

    @Test
    public void testSYSIBMSQLUDTS() throws Exception {
        try(Statement s = conn.createStatement()){
            try(ResultSet rs=s.executeQuery("CALL SYSIBM.SQLUDTS(null,null,null,null,null)")){
                resultSetSize(rs); //can't check because some other tests may add a UDT, which could result in a non-zero number
            }
        }
    }

    @Test
    public void testSYSIBMSQLTABLES() throws Exception {
        try(Statement s = conn.createStatement()){
            try(ResultSet rs=s.executeQuery("CALL SYSIBM.SQLTABLES(null,null,null,null,null)")){
                Assert.assertNotEquals("Incorrect rows returned!",0,resultSetSize(rs));
            }
        }
    }

    @Test
    public void testSYSIBMSQLTABLEPRIVILEGES() throws Exception {
        try(Statement s = conn.createStatement()){
            try(ResultSet rs=s.executeQuery("CALL SYSIBM.SQLTABLEPRIVILEGES(null,null,null,null)")){
                resultSetSize(rs);
            }
        }
    }

    @Test
    public void testSYSIBMSQLSTATISTICS() throws Exception {
        try(Statement s = conn.createStatement()){
            try(ResultSet rs=s.executeQuery("CALL SYSIBM.SQLSTATISTICS(null,null,null,0,1,null)")){
                resultSetSize(rs);
            }
        }
    }

    @Test
    public void testSYSIBMSQLSPECIALCOLUMNS() throws Exception {
        try(Statement s = conn.createStatement()){
            try(ResultSet rs = s.executeQuery("CALL SYSIBM.SQLSPECIALCOLUMNS(1,null,null,'FOO',0,0,null)")){
                resultSetSize(rs);
            }
        }
    }

    @Test
    public void testSQLPROCEDURES() throws Exception {
        try(Statement s = conn.createStatement()){
            try(ResultSet rs=s.executeQuery("CALL SYSIBM.SQLPROCEDURES(null,null,null,null)")){
                resultSetSize(rs);
            }
        }
    }

    @Test
    public void testSQLPROCEDURECOLS() throws Exception {
        try(Statement s = conn.createStatement()){
            try(ResultSet rs=s.executeQuery("CALL SYSIBM.SQLPROCEDURECOLS(null,null,null,null,null)")){
               resultSetSize(rs);
            }
        }
    }

    @Test
    public void testSQLPRIMARYKEYS() throws Exception {
        try(Statement s = conn.createStatement()){
            try(ResultSet rs=s.executeQuery("CALL SYSIBM.SQLPRIMARYKEYS(null,null,'FOO',null)")){
                resultSetSize(rs);
            }
        }
    }

    @Test
    public void testSQLGETTYPEINFO() throws Exception {
        try(Statement s = conn.createStatement()){
            try(ResultSet rs=s.executeQuery("CALL SYSIBM.SQLGETTYPEINFO(0,null)")){
               resultSetSize(rs);
            }
        }
    }

    @Test
    public void testSQLFUNCTIONS() throws Exception {
        try(Statement s = conn.createStatement()){
            try(ResultSet rs=s.executeQuery("CALL SYSIBM.SQLFUNCTIONS(null,null,null,null)")){
                resultSetSize(rs);
            }
        }
    }


    @Test
    public void testSQLFUNCTIONPARAMS() throws Exception {
        try(Statement s = conn.createStatement()){
            try(ResultSet rs=s.executeQuery("CALL SYSIBM.SQLFUNCTIONPARAMS(null,null,null,null,null)")){
               resultSetSize(rs);
            }
        }
    }

    @Test
    public void testSQLCOLUMNS() throws Exception {
        try(Statement s = conn.createStatement()){
            try(ResultSet rs=s.executeQuery("CALL SYSIBM.SQLCOLUMNS(null,null,null,null,null)")){
                resultSetSize(rs);
            }
        }
    }



    @Test
    public void testSQLCOLPRIVILEGES() throws Exception {
        try(Statement s = conn.createStatement()){
            try(ResultSet rs=s.executeQuery("CALL SYSIBM.SQLCOLPRIVILEGES(null,null,'FOO',null,null)")){
                resultSetSize(rs);
            }
        }
    }

    @Test
    public void testMETADATA() throws Exception {
        try(Statement s = conn.createStatement()){
            try(ResultSet rs = s.executeQuery("CALL SYSIBM.METADATA()")){
                resultSetSize(rs);
            }
        }
    }
}
