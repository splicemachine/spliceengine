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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceIndexWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.ResultSet;

/**
 * Created by tgildersleeve on 7/11/17.
 */
@Ignore
public class DefaultIndexIT {

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    private static final String CLASS_NAME = DefaultIndexIT.class.getSimpleName().toUpperCase();

    protected  static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    private static SpliceTableWatcher CHAR_TABLE = new SpliceTableWatcher("CHAR_TABLE", schemaWatcher.schemaName,"(i char default 'A', j char)");
    private static SpliceTableWatcher VARCHAR_TABLE = new SpliceTableWatcher("VARCHAR_TABLE", schemaWatcher.schemaName,"(i varchar(10) default 'AAA', j varchar(10))");
    private static SpliceTableWatcher BLANK_TABLE = new SpliceTableWatcher("BLANK_TABLE", schemaWatcher.schemaName,"(i varchar(10) default '', j varchar(10))");


    private static SpliceTableWatcher DATE_TABLE = new SpliceTableWatcher("DATE_TABLE", schemaWatcher.schemaName,"(i date default '2000-01-01', j date)");
    private static SpliceTableWatcher TIME_TABLE = new SpliceTableWatcher("TIME_TABLE", schemaWatcher.schemaName,"(i time default '00:00:00', j time)");
    private static SpliceTableWatcher TIMESTAMP_TABLE = new SpliceTableWatcher("TIMESTAMP_TABLE", schemaWatcher.schemaName,"(i timestamp default '2000-01-01 00:00:00', j timestamp)");

    private static SpliceTableWatcher BIGINT_TABLE = new SpliceTableWatcher("BIGINT_TABLE", schemaWatcher.schemaName,"(i bigint default 10, j bigint)");
    private static SpliceTableWatcher DECIMAL_TABLE = new SpliceTableWatcher("DECIMAL_TABLE", schemaWatcher.schemaName,"(i decimal default 10, j decimal)");
    private static SpliceTableWatcher DOUBLE_TABLE = new SpliceTableWatcher("DOUBLE_TABLE", schemaWatcher.schemaName,"(i double default 10, j double)");
    private static SpliceTableWatcher DOUBLE_PRECISION_TABLE = new SpliceTableWatcher("DOUBLE_PRECISION_TABLE", schemaWatcher.schemaName,"(i double precision default 10, j double precision)");
    private static SpliceTableWatcher FLOAT_TABLE = new SpliceTableWatcher("FLOAT_TABLE", schemaWatcher.schemaName,"(i float default 10, j float)");
    private static SpliceTableWatcher INTEGER_TABLE = new SpliceTableWatcher("INTEGER_TABLE", schemaWatcher.schemaName,"(i integer default 10, j integer)");
    private static SpliceTableWatcher NUMERIC_TABLE = new SpliceTableWatcher("NUMERIC_TABLE", schemaWatcher.schemaName,"(i numeric default 10, j numeric)");
    private static SpliceTableWatcher REAL_TABLE = new SpliceTableWatcher("REAL_TABLE", schemaWatcher.schemaName,"(i real default 10, j real)");
    private static SpliceTableWatcher SMALLINT_TABLE = new SpliceTableWatcher("SMALLINT_TABLE", schemaWatcher.schemaName,"(i smallint default 10, j smallint)");

    private static SpliceTableWatcher BOOLEAN_TABLE = new SpliceTableWatcher("BOOLEAN_TABLE", schemaWatcher.schemaName,"(i boolean default true, j boolean)");

    private static SpliceIndexWatcher CHAR_INDEX = new SpliceIndexWatcher("CHAR_TABLE", schemaWatcher.schemaName, "CHAR_INDEX", schemaWatcher.schemaName, "(i)");
    private static SpliceIndexWatcher VARCHAR_INDEX = new SpliceIndexWatcher("VARCHAR_TABLE", schemaWatcher.schemaName, "VARCHAR_INDEX", schemaWatcher.schemaName, "(i)");
    private static SpliceIndexWatcher BLANK_INDEX = new SpliceIndexWatcher("BLANK_TABLE", schemaWatcher.schemaName, "BLANK_INDEX", schemaWatcher.schemaName, "(i)");

    private static SpliceIndexWatcher DATE_INDEX = new SpliceIndexWatcher("DATE_TABLE", schemaWatcher.schemaName, "DATE_INDEX", schemaWatcher.schemaName, "(i)");
    private static SpliceIndexWatcher TIME_INDEX = new SpliceIndexWatcher("TIME_TABLE", schemaWatcher.schemaName, "TIME_INDEX", schemaWatcher.schemaName, "(i)");
    private static SpliceIndexWatcher TIMESTAMP_INDEX = new SpliceIndexWatcher("TIMESTAMP_TABLE", schemaWatcher.schemaName, "TIMESTAMP_INDEX", schemaWatcher.schemaName, "(i)");

    private static SpliceIndexWatcher BIGINT_INDEX = new SpliceIndexWatcher("BIGINT_TABLE", schemaWatcher.schemaName, "BIGINT_INDEX", schemaWatcher.schemaName, "(i)");
    private static SpliceIndexWatcher DECIMAL_INDEX = new SpliceIndexWatcher("DECIMAL_TABLE", schemaWatcher.schemaName, "DECIMAL_INDEX", schemaWatcher.schemaName, "(i)");
    private static SpliceIndexWatcher DOUBLE_INDEX = new SpliceIndexWatcher("DOUBLE_TABLE", schemaWatcher.schemaName, "DOUBLE_INDEX", schemaWatcher.schemaName, "(i)");
    private static SpliceIndexWatcher DOUBLE_PRECISION_INDEX = new SpliceIndexWatcher("DOUBLE_PRECISION_TABLE", schemaWatcher.schemaName, "DOUBLE_PRECISION_INDEX", schemaWatcher.schemaName, "(i)");
    private static SpliceIndexWatcher FLOAT_INDEX = new SpliceIndexWatcher("FLOAT_TABLE", schemaWatcher.schemaName, "FLOAT_INDEX", schemaWatcher.schemaName, "(i)");
    private static SpliceIndexWatcher INTEGER_INDEX = new SpliceIndexWatcher("INTEGER_TABLE", schemaWatcher.schemaName, "INTEGER_INDEX", schemaWatcher.schemaName, "(i)");
    private static SpliceIndexWatcher NUMERIC_INDEX = new SpliceIndexWatcher("NUMERIC_TABLE", schemaWatcher.schemaName, "NUMERIC_INDEX", schemaWatcher.schemaName, "(i)");
    private static SpliceIndexWatcher REAL_INDEX = new SpliceIndexWatcher("REAL_TABLE", schemaWatcher.schemaName, "REAL_INDEX", schemaWatcher.schemaName, "(i)");
    private static SpliceIndexWatcher SMALLINT_INDEX = new SpliceIndexWatcher("SMALLINT_TABLE", schemaWatcher.schemaName, "SMALLINT_INDEX", schemaWatcher.schemaName, "(i)");


    private static SpliceIndexWatcher BOOLEAN_INDEX = new SpliceIndexWatcher("BOOLEAN_TABLE", schemaWatcher.schemaName, "BOOLEAN_INDEX", schemaWatcher.schemaName, "(i)");

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(schemaWatcher.schemaName);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(schemaWatcher)
            .around(CHAR_TABLE)
            .around(VARCHAR_TABLE)
            .around(BLANK_TABLE)
            .around(DATE_TABLE)
            .around(TIME_TABLE)
            .around(TIMESTAMP_TABLE)
            .around(BIGINT_TABLE)
            .around(DECIMAL_TABLE)
            .around(DOUBLE_TABLE)
            .around(DOUBLE_PRECISION_TABLE)
            .around(FLOAT_TABLE)
            .around(INTEGER_TABLE)
            .around(NUMERIC_TABLE)
            .around(REAL_TABLE)
            .around(SMALLINT_TABLE)
            .around(BOOLEAN_TABLE)
            .around(CHAR_INDEX)
            .around(VARCHAR_INDEX)
            .around(BLANK_INDEX)
            .around(DATE_INDEX)
            .around(TIME_INDEX)
            .around(TIMESTAMP_INDEX)
            .around(BIGINT_INDEX)
            .around(DECIMAL_INDEX)
            .around(DOUBLE_INDEX)
            .around(DOUBLE_PRECISION_INDEX)
            .around(FLOAT_INDEX)
            .around(INTEGER_INDEX)
            .around(NUMERIC_INDEX)
            .around(REAL_INDEX)
            .around(SMALLINT_INDEX)
            .around(BOOLEAN_INDEX);

    private Connection conn;

    @Before
    public void setUpTest() throws Exception{
        conn=methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);
    }

    @After
    public void tearDownTest() throws Exception{
        conn.rollback();
    }

    @Before
    public void setUp() throws Exception {
        methodWatcher.executeUpdate(String.format("INSERT INTO CHAR_TABLE(J) VALUES ('A'),('A'),('A')"));
        methodWatcher.executeUpdate(String.format("INSERT INTO VARCHAR_TABLE(J) VALUES ('AAA'),('AAA'),('AAA')"));
        methodWatcher.executeUpdate(String.format("INSERT INTO BLANK_TABLE(J) VALUES (''),(''),('')"));

        methodWatcher.executeUpdate(String.format("INSERT INTO DATE_TABLE(J) VALUES ('2000-01-01'),('2000-01-01'),('2000-01-01')"));
        methodWatcher.executeUpdate(String.format("INSERT INTO TIME_TABLE(J) VALUES ('00:00:00'),('00:00:00'),('00:00:00')"));
        methodWatcher.executeUpdate(String.format("INSERT INTO TIMESTAMP_TABLE(J) VALUES ('2000-01-01 00:00:00'),('2000-01-01 00:00:00'),('2000-01-01 00:00:00')"));

        methodWatcher.executeUpdate(String.format("INSERT INTO BIGINT_TABLE(J) VALUES (10),(10),(10)"));
        methodWatcher.executeUpdate(String.format("INSERT INTO DECIMAL_TABLE(J) VALUES (10),(10),(10)"));
        methodWatcher.executeUpdate(String.format("INSERT INTO DOUBLE_TABLE(J) VALUES (10),(10),(10)"));
        methodWatcher.executeUpdate(String.format("INSERT INTO DOUBLE_PRECISION_TABLE(J) VALUES (10),(10),(10)"));
        methodWatcher.executeUpdate(String.format("INSERT INTO FLOAT_TABLE(J) VALUES (10),(10),(10)"));
        methodWatcher.executeUpdate(String.format("INSERT INTO INTEGER_TABLE(J) VALUES (10),(10),(10)"));
        methodWatcher.executeUpdate(String.format("INSERT INTO NUMERIC_TABLE(J) VALUES (10),(10),(10)"));
        methodWatcher.executeUpdate(String.format("INSERT INTO REAL_TABLE(J) VALUES (10),(10),(10)"));
        methodWatcher.executeUpdate(String.format("INSERT INTO SMALLINT_TABLE(J) VALUES (10),(10),(10)"));
        
        methodWatcher.executeUpdate(String.format("INSERT INTO BOOLEAN_TABLE(J) VALUES (true),(true),(true)"));
    }

    @Test
    public void CHAR_TEST() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM CHAR_TABLE --SPLICE-PROPERTIES index=CHAR_INDEX\n WHERE I = 'A'"));
        Assert.assertEquals("I | J |\n" +
                "--------\n" +
                " A | A |\n" +
                " A | A |\n" +
                " A | A |", TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet rs1 = methodWatcher.executeQuery(String.format("SELECT COUNT(I) FROM CHAR_TABLE --SPLICE-PROPERTIES index=CHAR_INDEX\n WHERE I = 'A'"));
        Assert.assertEquals("1 |\n" +
                "----\n" +
                " 3 |", TestUtils.FormattedResult.ResultFactory.toString(rs1));
    }

    @Test
    public void VARCHAR_TEST() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM VARCHAR_TABLE --SPLICE-PROPERTIES index=VARCHAR_INDEX\n WHERE I = 'AAA'"));
        Assert.assertEquals("I  | J  |\n" +
                "----------\n" +
                "AAA |AAA |\n" +
                "AAA |AAA |\n" +
                "AAA |AAA |", TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet rs1 = methodWatcher.executeQuery(String.format("SELECT COUNT(I) FROM VARCHAR_TABLE --SPLICE-PROPERTIES index=VARCHAR_INDEX\n WHERE I = 'AAA'"));
        Assert.assertEquals("1 |\n" +
                "----\n" +
                " 3 |", TestUtils.FormattedResult.ResultFactory.toString(rs1));
    }

    @Test
    public void BLANK_TEST() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM BLANK_TABLE --SPLICE-PROPERTIES index=BLANK_INDEX\n WHERE I = ''"));
        Assert.assertEquals("I | J |\n" +
                "--------\n" +
                "   |   |\n" +
                "   |   |\n" +
                "   |   |", TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet rs1 = methodWatcher.executeQuery(String.format("SELECT COUNT(I) FROM BLANK_TABLE --SPLICE-PROPERTIES index=BLANK_INDEX\n WHERE I = ''"));
        Assert.assertEquals("1 |\n" +
                "----\n" +
                " 3 |", TestUtils.FormattedResult.ResultFactory.toString(rs1));
    }

    @Test
    public void DATE_TEST() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM DATE_TABLE --SPLICE-PROPERTIES index=DATE_INDEX\n WHERE I = '2000-01-01'"));
        Assert.assertEquals("I     |     J     |\n" +
                "------------------------\n" +
                "2000-01-01 |2000-01-01 |\n" +
                "2000-01-01 |2000-01-01 |\n" +
                "2000-01-01 |2000-01-01 |", TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet rs1 = methodWatcher.executeQuery(String.format("SELECT COUNT(I) FROM DATE_TABLE --SPLICE-PROPERTIES index=DATE_INDEX\n WHERE I = '2000-01-01'"));
        Assert.assertEquals("1 |\n" +
                "----\n" +
                " 3 |", TestUtils.FormattedResult.ResultFactory.toString(rs1));
    }

    @Test
    public void TIME_TEST() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM TIME_TABLE --SPLICE-PROPERTIES index=TIME_INDEX\n WHERE I = '00:00:00'"));
        Assert.assertEquals("I    |    J    |\n" +
                "--------------------\n" +
                "00:00:00 |00:00:00 |\n" +
                "00:00:00 |00:00:00 |\n" +
                "00:00:00 |00:00:00 |", TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet rs1 = methodWatcher.executeQuery(String.format("SELECT COUNT(I) FROM TIME_TABLE --SPLICE-PROPERTIES index=TIME_INDEX\n WHERE I = '00:00:00'"));
        Assert.assertEquals("1 |\n" +
                "----\n" +
                " 3 |", TestUtils.FormattedResult.ResultFactory.toString(rs1));
    }

    @Test
    public void TIMESTAMP_TEST() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM TIMESTAMP_TABLE --SPLICE-PROPERTIES index=TIMESTAMP_INDEX\n WHERE I = '2000-01-01 00:00:00'"));
        Assert.assertEquals("I           |          J           |\n" +
                "----------------------------------------------\n" +
                "2000-01-01 00:00:00.0 |2000-01-01 00:00:00.0 |\n" +
                "2000-01-01 00:00:00.0 |2000-01-01 00:00:00.0 |\n" +
                "2000-01-01 00:00:00.0 |2000-01-01 00:00:00.0 |", TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet rs1 = methodWatcher.executeQuery(String.format("SELECT COUNT(I) FROM TIMESTAMP_TABLE --SPLICE-PROPERTIES index=TIMESTAMP_INDEX\n WHERE I = '2000-01-01 00:00:00'"));
        Assert.assertEquals("1 |\n" +
                "----\n" +
                " 3 |", TestUtils.FormattedResult.ResultFactory.toString(rs1));
    }

    @Test
    public void BIGINT_TEST() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM BIGINT_TABLE --SPLICE-PROPERTIES index=BIGINT_INDEX\n WHERE I = 10"));
        Assert.assertEquals("I | J |\n" +
                "--------\n" +
                "10 |10 |\n" +
                "10 |10 |\n" +
                "10 |10 |", TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet rs1 = methodWatcher.executeQuery(String.format("SELECT COUNT(I) FROM BIGINT_TABLE --SPLICE-PROPERTIES index=BIGINT_INDEX\n WHERE I = 10"));
        Assert.assertEquals("1 |\n" +
                "----\n" +
                " 3 |", TestUtils.FormattedResult.ResultFactory.toString(rs1));
    }

    @Test
    public void DECIMAL_TEST() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM DECIMAL_TABLE --SPLICE-PROPERTIES index=DECIMAL_INDEX\n WHERE I = 10"));
        Assert.assertEquals("I | J |\n" +
                "--------\n" +
                "10 |10 |\n" +
                "10 |10 |\n" +
                "10 |10 |", TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet rs1 = methodWatcher.executeQuery(String.format("SELECT COUNT(I) FROM DECIMAL_TABLE --SPLICE-PROPERTIES index=DECIMAL_INDEX\n WHERE I = 10"));
        Assert.assertEquals("1 |\n" +
                "----\n" +
                " 3 |", TestUtils.FormattedResult.ResultFactory.toString(rs1));
    }

    @Test
    public void DOUBLE_TEST() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM DOUBLE_TABLE --SPLICE-PROPERTIES index=DOUBLE_INDEX\n WHERE I = 10"));
        Assert.assertEquals("I  |  J  |\n" +
                "------------\n" +
                "10.0 |10.0 |\n" +
                "10.0 |10.0 |\n" +
                "10.0 |10.0 |", TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet rs1 = methodWatcher.executeQuery(String.format("SELECT COUNT(I) FROM DOUBLE_TABLE --SPLICE-PROPERTIES index=DOUBLE_INDEX\n WHERE I = 10"));
        Assert.assertEquals("1 |\n" +
                "----\n" +
                " 3 |", TestUtils.FormattedResult.ResultFactory.toString(rs1));
    }

    @Test
    public void DOUBLE_PRECISION_TEST() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM DOUBLE_PRECISION_TABLE --SPLICE-PROPERTIES index=DOUBLE_PRECISION_INDEX\n WHERE I = 10"));
        Assert.assertEquals("I  |  J  |\n" +
                "------------\n" +
                "10.0 |10.0 |\n" +
                "10.0 |10.0 |\n" +
                "10.0 |10.0 |", TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet rs1 = methodWatcher.executeQuery(String.format("SELECT COUNT(I) FROM DOUBLE_PRECISION_TABLE --SPLICE-PROPERTIES index=DOUBLE_PRECISION_INDEX\n WHERE I = 10"));
        Assert.assertEquals("1 |\n" +
                "----\n" +
                " 3 |", TestUtils.FormattedResult.ResultFactory.toString(rs1));
    }

    @Test
    public void FLOAT_TEST() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM FLOAT_TABLE --SPLICE-PROPERTIES index=FLOAT_INDEX\n WHERE I = 10"));
        Assert.assertEquals("I  |  J  |\n" +
                "------------\n" +
                "10.0 |10.0 |\n" +
                "10.0 |10.0 |\n" +
                "10.0 |10.0 |", TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet rs1 = methodWatcher.executeQuery(String.format("SELECT COUNT(I) FROM FLOAT_TABLE --SPLICE-PROPERTIES index=FLOAT_INDEX\n WHERE I = 10"));
        Assert.assertEquals("1 |\n" +
                "----\n" +
                " 3 |", TestUtils.FormattedResult.ResultFactory.toString(rs1));
    }
    @Test
    public void INTEGER_TEST() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM INTEGER_TABLE --SPLICE-PROPERTIES index=INTEGER_INDEX\n WHERE I = 10"));
        Assert.assertEquals("I | J |\n" +
                "--------\n" +
                "10 |10 |\n" +
                "10 |10 |\n" +
                "10 |10 |", TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet rs1 = methodWatcher.executeQuery(String.format("SELECT COUNT(I) FROM INTEGER_TABLE --SPLICE-PROPERTIES index=INTEGER_INDEX\n WHERE I = 10"));
        Assert.assertEquals("1 |\n" +
                "----\n" +
                " 3 |", TestUtils.FormattedResult.ResultFactory.toString(rs1));
    }
    @Test
    public void NUMERIC_TEST() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM NUMERIC_TABLE --SPLICE-PROPERTIES index=NUMERIC_INDEX\n WHERE I = 10"));
        Assert.assertEquals("I | J |\n" +
                "--------\n" +
                "10 |10 |\n" +
                "10 |10 |\n" +
                "10 |10 |", TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet rs1 = methodWatcher.executeQuery(String.format("SELECT COUNT(I) FROM NUMERIC_TABLE --SPLICE-PROPERTIES index=NUMERIC_INDEX\n WHERE I = 10"));
        Assert.assertEquals("1 |\n" +
                "----\n" +
                " 3 |", TestUtils.FormattedResult.ResultFactory.toString(rs1));
    }
    @Test
    public void REAL_TEST() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM REAL_TABLE --SPLICE-PROPERTIES index=REAL_INDEX\n WHERE I = 10"));
        Assert.assertEquals("I  |  J  |\n" +
                "------------\n" +
                "10.0 |10.0 |\n" +
                "10.0 |10.0 |\n" +
                "10.0 |10.0 |", TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet rs1 = methodWatcher.executeQuery(String.format("SELECT COUNT(I) FROM REAL_TABLE --SPLICE-PROPERTIES index=REAL_INDEX\n WHERE I = 10"));
        Assert.assertEquals("1 |\n" +
                "----\n" +
                " 3 |", TestUtils.FormattedResult.ResultFactory.toString(rs1));
    }
    @Test
    public void SMALLINT_TEST() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM SMALLINT_TABLE --SPLICE-PROPERTIES index=SMALLINT_INDEX\n WHERE I = 10"));
        Assert.assertEquals("I | J |\n" +
                "--------\n" +
                "10 |10 |\n" +
                "10 |10 |\n" +
                "10 |10 |", TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet rs1 = methodWatcher.executeQuery(String.format("SELECT COUNT(I) FROM SMALLINT_TABLE --SPLICE-PROPERTIES index=SMALLINT_INDEX\n WHERE I = 10"));
        Assert.assertEquals("1 |\n" +
                "----\n" +
                " 3 |", TestUtils.FormattedResult.ResultFactory.toString(rs1));
    }

    @Test
    public void BOOLEAN_TEST() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(String.format("SELECT * FROM BOOLEAN_TABLE --SPLICE-PROPERTIES index=BOOLEAN_INDEX\n WHERE I = true"));
        Assert.assertEquals("I  |  J  |\n" +
                "------------\n" +
                "true |true |\n" +
                "true |true |\n" +
                "true |true |", TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet rs1 = methodWatcher.executeQuery(String.format("SELECT COUNT(I) FROM BOOLEAN_TABLE --SPLICE-PROPERTIES index=BOOLEAN_INDEX\n WHERE I = true"));
        Assert.assertEquals("1 |\n" +
                "----\n" +
                " 3 |", TestUtils.FormattedResult.ResultFactory.toString(rs1));
    }


}
