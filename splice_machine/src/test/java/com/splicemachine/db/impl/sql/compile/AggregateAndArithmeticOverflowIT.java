/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.spark_project.guava.collect.Lists;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 * Test aggregations that used to overflow or produce incorrect results.
 * Test arithmetic expressions which used to overflow because the data
 * type of the result expression was poorly chosen.
 */
@RunWith(Parameterized.class)
public class AggregateAndArithmeticOverflowIT  extends SpliceUnitTest {
    
    private Boolean useSpark;
    
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{true});
        params.add(new Object[]{false});
        return params;
    }
    public static final String CLASS_NAME = AggregateAndArithmeticOverflowIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);
    
    public AggregateAndArithmeticOverflowIT(Boolean useSpark) {
        this.useSpark = useSpark;
    }
    
    public static void createData(Connection conn, String schemaName) throws Exception {

        new TableCreator(conn)
                .withCreate("create table ts_int (t tinyint, s smallint, i int, l bigint)")
                .withInsert("insert into ts_int values(?, ?, ?, ?)")
                .withRows(rows(
                        row(127, 32767, 2147483647, Long.MAX_VALUE),
                        row(127, 32767, 2147483647, Long.MAX_VALUE),
                        row(127, 32767, 2147483647, Long.MAX_VALUE),
                        row(127, 32767, 2147483647, Long.MAX_VALUE),
                        row(null, null, null, null),
                        row(null, null, null, null),
                        row(null, null, null, null)))
                .withIndex("create index ix_int on ts_int(l, i)")
                .create();

        new TableCreator(conn)
                .withCreate("create table ts_int2 (t tinyint, s smallint, i int, l bigint)")
                .withInsert("insert into ts_int2 values(?, ?, ?, ?)")
                .withRows(rows(
                        row(-128, -32768, -2147483648, Long.MIN_VALUE),
                        row(-128, -32768, -2147483648, Long.MIN_VALUE),
                        row(-128, -32768, -2147483648, Long.MIN_VALUE),
                        row(-128, -32768, -2147483648, Long.MIN_VALUE),
                        row(null, null, null, null),
                        row(null, null, null, null),
                        row(null, null, null, null)))
                .withIndex("create index ix_int2 on ts_int2(i, l)")
                .create();

        new TableCreator(conn)
                .withCreate("create table ts_bigint (b bigint, c int, d int)")
                .withInsert("insert into ts_bigint values(?, ?, ?)")
                .withRows(rows(
                        row(1000000000000000000L, 1, 1),
                        row(1000000000000000000L, 1, 2),
                        row(1000000000000000000L, 1, 3),
                        row(2000000000000000000L, 1, 4),
                        row(-1000000000000000000L, 1, 5),
                        row(null, null, null),
                        row(null, null, null)))
                .create();

        String sqlText = "insert into ts_bigint select * from ts_bigint";
		spliceClassWatcher.executeUpdate(sqlText);
		spliceClassWatcher.executeUpdate(sqlText);
		spliceClassWatcher.executeUpdate(sqlText);
		spliceClassWatcher.executeUpdate(sqlText);

        new TableCreator(conn)
                .withCreate("create table ts_bigint2 (b bigint, c int)")
                .withInsert("insert into ts_bigint2 values(?, ?)")
                .withRows(rows(
                        row(-1000000000000000000L, 1),
                        row(-1000000000000000000L, 1),
                        row(-1000000000000000000L, 1),
                        row(-1000000000000000000L, 1),
                        row(-1000000000000000000L, 1),
                        row(null, null),
                        row(null, null)))
                .create();

        sqlText = "insert into ts_bigint2 select * from ts_bigint2";
		spliceClassWatcher.executeUpdate(sqlText);
		spliceClassWatcher.executeUpdate(sqlText);
		spliceClassWatcher.executeUpdate(sqlText);
		spliceClassWatcher.executeUpdate(sqlText);

		// For testing merge of aggregation results.
        new TableCreator(conn)
                .withCreate("create table ts_bigint3 (b bigint, c int, primary key(c))")
                .withInsert("insert into ts_bigint3 values(?, ?)")
                .withRows(rows(
                        row(1, 1)))
                .create();

        sqlText = "insert into ts_bigint3 select b, c+1 from ts_bigint3";
        spliceClassWatcher.executeUpdate(sqlText);
        sqlText = "insert into ts_bigint3 select b, c+2 from ts_bigint3";
		spliceClassWatcher.executeUpdate(sqlText);
        sqlText = "insert into ts_bigint3 select b, c+4 from ts_bigint3";
		spliceClassWatcher.executeUpdate(sqlText);
        sqlText = "insert into ts_bigint3 select b, c+8 from ts_bigint3";
		spliceClassWatcher.executeUpdate(sqlText);
        sqlText = "insert into ts_bigint3 select b, c+16 from ts_bigint3";
		spliceClassWatcher.executeUpdate(sqlText);
        sqlText = "insert into ts_bigint3 select b, c+32 from ts_bigint3";
		spliceClassWatcher.executeUpdate(sqlText);
        sqlText = "insert into ts_bigint3 select b, c+64 from ts_bigint3";
		spliceClassWatcher.executeUpdate(sqlText);
        sqlText = "insert into ts_bigint3 select b, c+128 from ts_bigint3";
		spliceClassWatcher.executeUpdate(sqlText);
        sqlText = "insert into ts_bigint3 select b, c+256 from ts_bigint3";
		spliceClassWatcher.executeUpdate(sqlText);
        sqlText = "insert into ts_bigint3 select b, c+512 from ts_bigint3";
		spliceClassWatcher.executeUpdate(sqlText);
		// 1K rows have been inserted

        sqlText = "insert into ts_bigint3 select b, c+1024 from ts_bigint3";
        spliceClassWatcher.executeUpdate(sqlText);
        sqlText = "insert into ts_bigint3 select b, c+2048 from ts_bigint3";
		spliceClassWatcher.executeUpdate(sqlText);
        sqlText = "insert into ts_bigint3 select b, c+4096 from ts_bigint3";
		spliceClassWatcher.executeUpdate(sqlText);
        sqlText = "insert into ts_bigint3 select b, c+8192 from ts_bigint3";
		spliceClassWatcher.executeUpdate(sqlText);
        sqlText = "insert into ts_bigint3 select b, c+16384 from ts_bigint3";
		spliceClassWatcher.executeUpdate(sqlText);
        sqlText = "insert into ts_bigint3 select b, c+32768 from ts_bigint3";
		spliceClassWatcher.executeUpdate(sqlText);
        sqlText = "insert into ts_bigint3 select b, c+65536 from ts_bigint3";
		spliceClassWatcher.executeUpdate(sqlText);
        sqlText = "insert into ts_bigint3 select b, c+131072 from ts_bigint3";
		spliceClassWatcher.executeUpdate(sqlText);
        sqlText = "insert into ts_bigint3 select b, c+262144 from ts_bigint3";
		spliceClassWatcher.executeUpdate(sqlText);
        sqlText = "insert into ts_bigint3 select 10000000000000000, c+524288 from ts_bigint3";
		spliceClassWatcher.executeUpdate(sqlText);
		// 1M rows have been inserted

        new TableCreator(conn)
        .withCreate("create table MY_TABLE\n" +
                    "(\n" +
                    "        SALES_AMT DECIMAL(9,2),\n" +
                    "        SALES_QTY INTEGER,\n" +
                    "        ORIGINAL_SKU_CATEGORY_ID INTEGER,\n" +
                    "        TRANSACTION_DT DATE not null,\n" +
                    "        SKU_NBR INTEGER,\n" +
                    "        UPC_ID BIGINT,\n" +
                    "        DIVISION_DESC VARCHAR(40),\n" +
                    "        DIVISION_ID INTEGER,\n" +
                    "        STORE_NBR SMALLINT not null,\n" +
                    "        STORE_NAME VARCHAR(40),\n" +
                    "        SKU_DESC VARCHAR(40),\n" +
                    "        SOURCE_SALES_INSTANCE_ID BIGINT,\n" +
                    "        BRAND_NAME VARCHAR(30),\n" +
                    "        CONTENTS_UNITS VARCHAR(10)\n" +
                    ")\n")
        .create();
        String rows[] = {"329.18, 9, 44871, '2013-05-12', 47008, 706378384, 'pOxYjQyqCNcWHyqlndvDGuSiHMZBiZNTMmjMLlUu', 942, 1780, 'tvgTJaHeYlmajcYVCiuohCnqHZVUBUhxoxKpApNz', 'VCginDZzAxPHDOCUuAmHSbilEOksPeAMtvLbbJXn', 0, 'jQDylYCJAzqWTUomBqynJkFnpdVnXV', 'jXtURzSHlc'",
        "35.46, 1, 44199, '2013-05-12', 83491, 514661132, 'QxtGiqjHUBFkXlbTXjnkwxbolneOkxibOXJVLCPc', 258, 1780, 'tvgTJaHeYlmajcYVCiuohCnqHZVUBUhxoxKpApNz', 'jLzRYEeMxOaTiOjngGcZUWlDPDKUAANTITIkrjzJ', 0, 'WzEERVNNGyHfuWoJNwUAkqWwYwZPYm', 'svnZHluXMq'",
        "395.44, 2, 44238, '2013-05-12', 33868, 349162963, 'NTBqyziiuCDuQdqSBATokjRvBmertuPaEEaEcqWh', 659, 1780, 'tvgTJaHeYlmajcYVCiuohCnqHZVUBUhxoxKpApNz', 'ftHaNZRFzCXlWHImwMNvWDFmWNLMTlEEaHJxwLUv', 0, 'ExdNbcGfrcvIchKngSfZNdMtcGCSDj', 'RnBCbnQGSR'",
        "1763.41, 8, 44410, '2013-05-12', 52315, 15756343, 'jaYIaLuHFbNJicBuLIiZyKnqXMOnBucqrkIVkbWp', 789, 1780, 'tvgTJaHeYlmajcYVCiuohCnqHZVUBUhxoxKpApNz', 'BQwlHbJgmdrGulvgvBnVIUlorWTGOVKuwmGKYNxA', 0, 'YiMzblArnFKTKXigpREReokDVfLAgu', 'kirDoVHOPV'",
        "915.97, 4, 44797, '2013-05-12', 59186, 284481177, 'KvUyqCvDBNuQQkYgaDxLeaIlBkwNWpOmSOcALXWf', 571, 1780, 'tvgTJaHeYlmajcYVCiuohCnqHZVUBUhxoxKpApNz', 'TpiOZxkWLGJYOBzhNjnNGHFTfdSjqRgtesXMfCcG', 0, 'BaPrtSrqHqhnsRQtksfzwJhbJmAmYq', 'XDMeYqgVDW'",
        "179.88, 3, 44837, '2013-05-12', 28020, 245664861, 'PLkmcBTYrfbxhcnnZVBVZynniSxpKSuLEErAyfRF', 211, 1780, 'tvgTJaHeYlmajcYVCiuohCnqHZVUBUhxoxKpApNz', 'cotFzzWvBNPgbPJZECfVduWhjSqvDEtpZBmiORJw', 0, 'UrIGqcCOVHHuFXPrxAWwlCYkzvIBby', 'vovFZYzSHC'",
        "0.00, 0, 44600, '2013-05-12', 46524, 613502555, 'ZYUNzfKfutcgsAqRUpyebCCjTfZyZsnNgKpWenEZ', 864, 1780, 'tvgTJaHeYlmajcYVCiuohCnqHZVUBUhxoxKpApNz', 'THIbSyDwFLRLfoTTIAGGOgWWgZKYHsccmKHjzMxy', 0, 'vRNJxoUZgawdSjKKVXwdlUvzGnKwBn', 'ibwEwHYOwY'"};

        for (int i = 0; i < rows.length; i++) {
            sqlText = String.format("insert into MY_TABLE values(%s)", rows[i]);
            spliceClassWatcher.executeUpdate(sqlText);
        }

        new TableCreator(conn)
                .withCreate("create table ts_decimal (a dec(11,1), b int)")
                .withInsert("insert into ts_decimal values(?, ?)")
                .withRows(rows(
                        row(9999999999.7, 1),
                        row(9999999999.9, 1),
                        row(9999999999.9, 1)))
                .create();

        new TableCreator(conn)
                .withCreate("create table ts_decimal2 (b dec(38,0), c int)")
                .withInsert("insert into ts_decimal2 values(?, ?)")
                .withRows(rows(
                        row(new BigDecimal("10000000000000000000000000000000"), 1),
                        row(new BigDecimal("100000000000000000000000000000000"), 1),
                        row(new BigDecimal("1000000000000000000000000000000000"), 1),
                        row(new BigDecimal("10000000000000000000000000000000000"), 1),
                        row(new BigDecimal("100000000000000000000000000000000000"), 1),
                        row(new BigDecimal("1000000000000000000000000000000000000"), 1),
                        row(new BigDecimal("10000000000000000000000000000000000000"), 1),
                        row(new BigDecimal("10000000000000000000000000000000000000"), 1),
                        row(new BigDecimal("10000000000000000000000000000000000000"), 1),
                        row(new BigDecimal("10000000000000000000000000000000000000"), 1),
                        row(new BigDecimal("10000000000000000000000000000000000000"), 1),
                        row(new BigDecimal("10000000000000000000000000000000000000"), 1),
                        row(new BigDecimal("10000000000000000000000000000000000000"), 1),
                        row(new BigDecimal("10000000000000000000000000000000000000"), 1),
                        row(new BigDecimal("10000000000000000000000000000000000000"), 1)))
                .create();

        new TableCreator(conn)
                .withCreate("create table ts_decimal3 (b dec(38,0), c int)")
                .create();

        sqlText = "insert into ts_decimal3 select * from ts_decimal2";
		spliceClassWatcher.executeUpdate(sqlText);
		spliceClassWatcher.executeUpdate(sqlText);

        new TableCreator(conn)
                .withCreate("create table ts_float (f1 float(40), f2 float(45))")
                .withInsert("insert into ts_float values(?, ?)")
                .withRows(rows(
                        row(3.11111111111E+8, 3.111111111111111111E+8),
                        row(3.11111111111E+28, 3.111111111111111111E+3),
                        row(3.11111111111E+18, 3.111111111111111111E+13),
                        row(3.11111111111E+54, 3.111111111111111111E+72),
                        row(null, null),
                        row(null, null),
                        row(null, null)))
                .create();

        new TableCreator(conn)
                .withCreate("create table ts_real (b real, c int)")
                .withInsert("insert into ts_real values(?, ?)")
                .withRows(rows(
                        row(3.402E+8,1),
                        row(-3.402E+8,1),
                        row(null,1),
                        row(1.175E-7,1)))
                .create();

        new TableCreator(conn)
                .withCreate("create table ts_real2 (b real, c int)")
                .withInsert("insert into ts_real2 values(?, ?)")
                .withRows(rows(
                        row(3.402E+38, 1),
                        row(3.402E+38, 1),
                        row(3.402E+38, 1),
                        row(3.402E+38, 1),
                        row(null,1),
                        row(1.175E-7,1)))
                .create();

    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
    }

    @Test
    public void test_DB_8082() throws Exception {
        String sqlText = format("select\n" +
                "min(TRANSACTION_DT) over (partition by ORIGINAL_SKU_CATEGORY_ID) C1,\n" +
                "sum(SALES_QTY) over (partition by TRANSACTION_DT,SKU_NBR,UPC_ID,DIVISION_DESC,DIVISION_ID," +
                "STORE_NBR,STORE_NAME,SKU_DESC,SOURCE_SALES_INSTANCE_ID,BRAND_NAME,CONTENTS_UNITS) C11\n" +
                "from MY_TABLE --splice-properties useSpark=%s", useSpark);

        String expected =
            "C1     | C11 |\n" +
            "------------------\n" +
            "2013-05-12 |  0  |\n" +
            "2013-05-12 |  1  |\n" +
            "2013-05-12 |  2  |\n" +
            "2013-05-12 |  3  |\n" +
            "2013-05-12 |  4  |\n" +
            "2013-05-12 |  8  |\n" +
            "2013-05-12 |  9  |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select\n" +
            "min(TRANSACTION_DT) over (partition by ORIGINAL_SKU_CATEGORY_ID) C1,\n" +
            "sum(SALES_QTY+SALES_QTY-2) over (partition by TRANSACTION_DT,SKU_NBR,UPC_ID,DIVISION_DESC,DIVISION_ID," +
            "STORE_NBR,STORE_NAME,SKU_DESC,SOURCE_SALES_INSTANCE_ID,BRAND_NAME,CONTENTS_UNITS) C11\n" +
            "from MY_TABLE --splice-properties useSpark=%s", useSpark);

        expected =
            "C1     | C11 |\n" +
            "------------------\n" +
            "2013-05-12 | -2  |\n" +
            "2013-05-12 |  0  |\n" +
            "2013-05-12 | 14  |\n" +
            "2013-05-12 | 16  |\n" +
            "2013-05-12 |  2  |\n" +
            "2013-05-12 |  4  |\n" +
            "2013-05-12 |  6  |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select\n" +
            "min(TRANSACTION_DT) over (partition by ORIGINAL_SKU_CATEGORY_ID) C1,\n" +
            "avg(SALES_QTY+SALES_QTY-2) over (partition by TRANSACTION_DT,SKU_NBR,UPC_ID,DIVISION_DESC,DIVISION_ID," +
            "STORE_NBR,STORE_NAME,SKU_DESC,SOURCE_SALES_INSTANCE_ID,BRAND_NAME,CONTENTS_UNITS) C11\n" +
            "from MY_TABLE --splice-properties useSpark=%s", useSpark);

        expected =
            "C1     |  C11   |\n" +
            "---------------------\n" +
            "2013-05-12 |-2.0000 |\n" +
            "2013-05-12 |0.0000  |\n" +
            "2013-05-12 |14.0000 |\n" +
            "2013-05-12 |16.0000 |\n" +
            "2013-05-12 |2.0000  |\n" +
            "2013-05-12 |4.0000  |\n" +
            "2013-05-12 |6.0000  |";

        testQuery(sqlText, expected, methodWatcher);
    }

    @Test
    public void testAvg() throws Exception {
        // Native spark execution is sensitive to precision loss from
        // differing orders of source rows.  DB-7960 fixes this issue
        // in Splice, but we have no control over native spark execution,
        // so skip these tests on Spark for now...
        if (useSpark)
            return;
        String sqlText = format("select avg(distinct  b/100) from ts_real --splice-properties useSpark=%s", useSpark);

        String expected =
                "1           |\n" +
                "------------------------\n" +
                "3.9166666666666666E-10 |";

        testQuery(sqlText, expected, methodWatcher);
        sqlText = format("select avg(b/100) from ts_real --splice-properties useSpark=%s", useSpark);
        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select avg(distinct  b/100) from ts_real2 --splice-properties useSpark=%s", useSpark);

        expected =
                "1           |\n" +
                "-----------------------\n" +
                "1.7009999999999999E36 |";

        testQuery(sqlText, expected, methodWatcher);
        sqlText = format("select avg(b/100) from ts_real2 --splice-properties useSpark=%s", useSpark);
        expected =
                "1           |\n" +
                "-----------------------\n" +
                "2.7215999999999996E36 |";
        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select avg(l) from ts_int --splice-properties useSpark=%s", useSpark);

        expected =
                "1          |\n" +
                "---------------------\n" +
                "9223372036854775807 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select avg(l) from ts_int2 --splice-properties useSpark=%s", useSpark);

        expected =
                "1          |\n" +
                "----------------------\n" +
                "-9223372036854775808 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select avg(i*i) from ts_int2 --splice-properties useSpark=%s", useSpark);

        expected =
                "1          |\n" +
                "---------------------\n" +
                "4611686018427387904 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select avg(i*l) from ts_int2 --splice-properties useSpark=%s", useSpark);
        expected =
                "1                 |\n" +
                        "------------------------------------\n" +
                        "19807040628566084398385987584.0000 |";
        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select avg(a) from ts_decimal --splice-properties useSpark=%s", useSpark);

        expected =
                "1        |\n" +
                "-----------------\n" +
                "9999999999.8333 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select avg(a*a) from ts_decimal --splice-properties useSpark=%s", useSpark);

        expected =
                "1             |\n" +
                "---------------------------\n" +
                "99999999996666666666.7033 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select avg(b) from ts_bigint --splice-properties useSpark=%s", useSpark);

        expected =
                "1         |\n" +
                "--------------------\n" +
                "800000000000000000 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select avg(b) from ts_bigint2 --splice-properties useSpark=%s", useSpark);

        expected =
                "1          |\n" +
                "----------------------\n" +
                "-1000000000000000000 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select avg(b) from ts_decimal2 --splice-properties useSpark=%s", useSpark);

        expected =
                "1                   |\n" +
                "---------------------------------------\n" +
                "6074074000000000000000000000000000000 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select avg(b) from ts_decimal3 --splice-properties useSpark=%s", useSpark);

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select avg(distinct f1/100) from ts_float --splice-properties useSpark=%s", useSpark);

        expected =
                "1         |\n" +
                "-------------------\n" +
                "7.777777777775E51 |";

        testQuery(sqlText, expected, methodWatcher);
        sqlText = format("select avg(f1/100) from ts_float --splice-properties useSpark=%s", useSpark);
        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select avg(distinct f2/100) from ts_float --splice-properties useSpark=%s", useSpark);

        expected =
                "1          |\n" +
                "----------------------\n" +
                "7.777777777777778E69 |";

        testQuery(sqlText, expected, methodWatcher);
        sqlText = format("select avg(f2/100) from ts_float --splice-properties useSpark=%s", useSpark);
        testQuery(sqlText, expected, methodWatcher);

        // To test aggregator upgrade during a merge of aggregation results of different data types.
        sqlText = format("select avg(b) from ts_bigint3 --splice-properties useSpark=%s", useSpark);

        expected =
                "1        |\n" +
                "------------------\n" +
                "5000000000000001 |";

        testQuery(sqlText, expected, methodWatcher);
    }

    @Test
    public void testSum() throws Exception {
        // Native spark execution is sensitive to precision loss from
        // differing orders of source rows.  DB-7960 fixes this issue
        // in Splice, but we have no control over native spark execution,
        // so skip these tests on Spark for now...
        if (useSpark)
            return;
        String sqlText = format("select sum(distinct  b/100) from ts_real --splice-properties useSpark=%s", useSpark);

        String expected =
                "1    |\n" +
                "----------\n" +
                "1.175E-9 |";

        testQuery(sqlText, expected, methodWatcher);
        sqlText = format("select sum(b/100) from ts_real --splice-properties useSpark=%s", useSpark);
        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select sum(distinct  b/100) from ts_real2 --splice-properties useSpark=%s", useSpark);

        expected =
                "1           |\n" +
                "-----------------------\n" +
                "3.4019999999999997E36 |";

        testQuery(sqlText, expected, methodWatcher);
        sqlText = format("select sum(b/100) from ts_real2 --splice-properties useSpark=%s", useSpark);
        expected =
                "1           |\n" +
                "-----------------------\n" +
                "1.3607999999999999E37 |";
        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select sum(l) from ts_int --splice-properties useSpark=%s", useSpark);

        expected =
                "1          |\n" +
                "----------------------\n" +
                "36893488147419103228 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select sum(l) from ts_int2 --splice-properties useSpark=%s", useSpark);

        expected =
                "1           |\n" +
                "-----------------------\n" +
                "-36893488147419103232 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select sum(i*i) from ts_int2 --splice-properties useSpark=%s", useSpark);

        expected =
                "1          |\n" +
                "----------------------\n" +
                "18446744073709551616 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select sum(i*l) from ts_int2 --splice-properties useSpark=%s", useSpark);

        expected =
                "1               |\n" +
                "-------------------------------\n" +
                "79228162514264337593543950336 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select sum(a) from ts_decimal --splice-properties useSpark=%s", useSpark);

        expected =
                "1       |\n" +
                "---------------\n" +
                "29999999999.5 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select sum(a*a) from ts_decimal --splice-properties useSpark=%s", useSpark);

        expected =
                "1            |\n" +
                "--------------------------\n" +
                "299999999990000000000.11 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select sum(b) from ts_bigint --splice-properties useSpark=%s", useSpark);

        expected =
                "1          |\n" +
                "----------------------\n" +
                "64000000000000000000 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select sum(b) from ts_bigint2 --splice-properties useSpark=%s", useSpark);

        expected =
                "1           |\n" +
                "-----------------------\n" +
                "-80000000000000000000 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select sum(b) from ts_decimal2 --splice-properties useSpark=%s", useSpark);

        expected =
                "1                   |\n" +
                "----------------------------------------\n" +
                "91111110000000000000000000000000000000 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select sum(b) from ts_decimal3 --splice-properties useSpark=%s", useSpark);

        List<String> expectedErrors =
           Arrays.asList("Overflow occurred during numeric data type conversion of \"1.8222222E+38\".",
                         "The numeric literal \"182222220000000000000000000000000000000\" is not valid because its value is out of range.");
        testFail(sqlText, expectedErrors, methodWatcher);

        sqlText = format("select sum(distinct f1/100) from ts_float --splice-properties useSpark=%s", useSpark);

        expected =
                "1        |\n" +
                "------------------\n" +
                "3.11111111111E52 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select sum(f1) from ts_float --splice-properties useSpark=%s", useSpark);

        expected =
                "1        |\n" +
                "------------------\n" +
                "3.11111111111E54 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select sum(distinct f2/100) from ts_float --splice-properties useSpark=%s", useSpark);

        expected =
                "1          |\n" +
                "----------------------\n" +
                "3.111111111111111E70 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select sum(f2) from ts_float --splice-properties useSpark=%s", useSpark);

        expected =
                "1          |\n" +
                "----------------------\n" +
                "3.111111111111111E72 |";

        testQuery(sqlText, expected, methodWatcher);

        // To test aggregator upgrade during a merge of aggregation results of different data types.
        sqlText = format("select sum(b) from ts_bigint3 --splice-properties useSpark=%s", useSpark);

        expected =
                "1           |\n" +
                "------------------------\n" +
                "5242880000000000524288 |";

        testQuery(sqlText, expected, methodWatcher);
    }

    @Test
    public void testArithmetic() throws Exception {
        String sqlText = format("select b+b+b+b from ts_real --splice-properties useSpark=%s", useSpark);

        String expected =
                "1     |\n" +
                "-----------\n" +
                "-1.3608E9 |\n" +
                "1.3608E9  |\n" +
                " 4.7E-7   |\n" +
                "  NULL    |";

        testQuery(sqlText, expected, methodWatcher);
        sqlText = format("select b+b+b+b*c/b from ts_real --splice-properties useSpark=%s", useSpark);
        expected =
                "1       |\n" +
                "----------------\n" +
                "-1.020599999E9 |\n" +
                " 1.0000003525  |\n" +
                " 1.020600001E9 |\n" +
                "     NULL      |";
        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select b+b+b+b from ts_real2 --splice-properties useSpark=%s", useSpark);

        expected =
                "1     |\n" +
                "-----------\n" +
                "1.3608E39 |\n" +
                "1.3608E39 |\n" +
                "1.3608E39 |\n" +
                "1.3608E39 |\n" +
                " 4.7E-7   |\n" +
                "  NULL    |";

        testQuery(sqlText, expected, methodWatcher);
        sqlText = format("select b+b+b+b*c/b from ts_real2 --splice-properties useSpark=%s", useSpark);
        expected =
                "1      |\n" +
                "--------------\n" +
                "1.0000003525 |\n" +
                "  1.0206E39  |\n" +
                "  1.0206E39  |\n" +
                "  1.0206E39  |\n" +
                "  1.0206E39  |\n" +
                "    NULL     |";
        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select t+t+t+t from ts_int --splice-properties useSpark=%s", useSpark);

        expected =
                "1  |\n" +
                "------\n" +
                " 508 |\n" +
                " 508 |\n" +
                " 508 |\n" +
                " 508 |\n" +
                "NULL |\n" +
                "NULL |\n" +
                "NULL |";

        testQuery(sqlText, expected, methodWatcher);
        sqlText = format("select t+t+t+t*l/i from ts_int --splice-properties useSpark=%s", useSpark);
        expected =
                "1         |\n" +
                "-------------------\n" +
                "545460847227.0000 |\n" +
                "545460847227.0000 |\n" +
                "545460847227.0000 |\n" +
                "545460847227.0000 |\n" +
                "      NULL        |\n" +
                "      NULL        |\n" +
                "      NULL        |";
        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select t+t+t+t from ts_int2 --splice-properties useSpark=%s", useSpark);

        expected =
                "1  |\n" +
                "------\n" +
                "-512 |\n" +
                "-512 |\n" +
                "-512 |\n" +
                "-512 |\n" +
                "NULL |\n" +
                "NULL |\n" +
                "NULL |";

        testQuery(sqlText, expected, methodWatcher);
        sqlText = format("select t+t+t+t*l/i from ts_int2 --splice-properties useSpark=%s", useSpark);
        expected =
                "1         |\n" +
                "--------------------\n" +
                "-549755814272.0000 |\n" +
                "-549755814272.0000 |\n" +
                "-549755814272.0000 |\n" +
                "-549755814272.0000 |\n" +
                "       NULL        |\n" +
                "       NULL        |\n" +
                "       NULL        |";
        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select s+s+s+s from ts_int --splice-properties useSpark=%s", useSpark);

        expected =
                "1   |\n" +
                "--------\n" +
                "131068 |\n" +
                "131068 |\n" +
                "131068 |\n" +
                "131068 |\n" +
                " NULL  |\n" +
                " NULL  |\n" +
                " NULL  |";

        testQuery(sqlText, expected, methodWatcher);
        sqlText = format("select s+s+s+s*l/i from ts_int --splice-properties useSpark=%s", useSpark);
        expected =
                "1          |\n" +
                "----------------------\n" +
                "140733193551867.0000 |\n" +
                "140733193551867.0000 |\n" +
                "140733193551867.0000 |\n" +
                "140733193551867.0000 |\n" +
                "        NULL         |\n" +
                "        NULL         |\n" +
                "        NULL         |";
        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select s+s+s+s from ts_int2 --splice-properties useSpark=%s", useSpark);

        expected =
                "1    |\n" +
                "---------\n" +
                "-131072 |\n" +
                "-131072 |\n" +
                "-131072 |\n" +
                "-131072 |\n" +
                " NULL   |\n" +
                " NULL   |\n" +
                " NULL   |";

        testQuery(sqlText, expected, methodWatcher);
        sqlText = format("select s+s+s+s*l/i from ts_int2 --splice-properties useSpark=%s", useSpark);
        expected =
                "1           |\n" +
                "-----------------------\n" +
                "-140737488453632.0000 |\n" +
                "-140737488453632.0000 |\n" +
                "-140737488453632.0000 |\n" +
                "-140737488453632.0000 |\n" +
                "        NULL          |\n" +
                "        NULL          |\n" +
                "        NULL          |";
        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select a+a+a+a+a+a+a+a+a+a+a+a+a+a+a+a from ts_decimal --splice-properties useSpark=%s", useSpark);

        expected =
                "1       |\n" +
                "----------------\n" +
                "159999999995.2 |\n" +
                "159999999998.4 |\n" +
                "159999999998.4 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select a+a+a+a+a+a+a+a+a+a+a+a+a+a+a+a/3 from ts_decimal --splice-properties useSpark=%s", useSpark);

        // For now, this query is expected to overflow.
        List<String> expectedErrors =
          Arrays.asList("The resulting value is outside the range for the data type DECIMAL/NUMERIC(38,28).");
          testFail(sqlText, expectedErrors, methodWatcher);

        sqlText = format("select f1+f1+f1+f1+f1+f1+f1+f1+f1+f1+f1 from ts_float --splice-properties useSpark=%s", useSpark);

        expected =
                "1           |\n" +
                "-----------------------\n" +
                "3.4222222222209995E55 |\n" +
                "3.4222222222210003E19 |\n" +
                "  3.422222222221E29   |\n" +
                "  3.422222222221E9    |\n" +
                "        NULL          |\n" +
                "        NULL          |\n" +
                "        NULL          |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select f2+f2+f2+f2+f2+f2+f2+f2+f2+f2+f2 from ts_float --splice-properties useSpark=%s", useSpark);

        expected =
                "1          |\n" +
                "----------------------\n" +
                "3.4222222222222223E9 |\n" +
                "3.422222222222222E73 |\n" +
                "3.422222222222223E14 |\n" +
                "  34222.22222222222  |\n" +
                "        NULL         |\n" +
                "        NULL         |\n" +
                "        NULL         |";

        testQuery(sqlText, expected, methodWatcher);
    }

    @Test
    public void testNativeSparkJoinsWithMax() throws Exception {
        String sqlText = format("select max(a.b+b.b) from ts_bigint a, ts_bigint b --splice-properties useSpark=%s\n" +
        "where a.c = b.c", useSpark);

        String expected =
        "1          |\n" +
        "---------------------\n" +
        "4000000000000000000 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select max(a.b+b.b) from ts_bigint a, ts_bigint b --splice-properties useSpark=%s\n" +
        "where a.c = b.c group by b.d", useSpark);

        expected =
        "1          |\n" +
        "---------------------\n" +
        "1000000000000000000 |\n" +
        "3000000000000000000 |\n" +
        "3000000000000000000 |\n" +
        "3000000000000000000 |\n" +
        "4000000000000000000 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select max(a.b+b.b) from ts_bigint a, ts_bigint b --splice-properties useSpark=%s\n" +
        "where a.c = b.c group by rollup(b.d)", useSpark);

        expected =
        "1          |\n" +
        "---------------------\n" +
        "1000000000000000000 |\n" +
        "3000000000000000000 |\n" +
        "3000000000000000000 |\n" +
        "3000000000000000000 |\n" +
        "4000000000000000000 |\n" +
        "4000000000000000000 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select max(a.b+b.b) from ts_bigint a, ts_bigint b --splice-properties useSpark=%s\n" +
        "where a.c = b.c group by rollup(a.d, b.d)", useSpark);

        expected =
        "1          |\n" +
        "----------------------\n" +
        "-2000000000000000000 |\n" +
        "          0          |\n" +
        "          0          |\n" +
        "          0          |\n" +
        "          0          |\n" +
        "          0          |\n" +
        "          0          |\n" +
        " 1000000000000000000 |\n" +
        " 1000000000000000000 |\n" +
        " 1000000000000000000 |\n" +
        " 2000000000000000000 |\n" +
        " 2000000000000000000 |\n" +
        " 2000000000000000000 |\n" +
        " 2000000000000000000 |\n" +
        " 2000000000000000000 |\n" +
        " 2000000000000000000 |\n" +
        " 2000000000000000000 |\n" +
        " 2000000000000000000 |\n" +
        " 2000000000000000000 |\n" +
        " 3000000000000000000 |\n" +
        " 3000000000000000000 |\n" +
        " 3000000000000000000 |\n" +
        " 3000000000000000000 |\n" +
        " 3000000000000000000 |\n" +
        " 3000000000000000000 |\n" +
        " 3000000000000000000 |\n" +
        " 3000000000000000000 |\n" +
        " 3000000000000000000 |\n" +
        " 4000000000000000000 |\n" +
        " 4000000000000000000 |\n" +
        " 4000000000000000000 |";

        testQuery(sqlText, expected, methodWatcher);
    }

    @Test
    public void testNativeSparkJoinsWithMin() throws Exception {
        String sqlText = format("select min(a.b+b.b) from ts_bigint a, ts_bigint b --splice-properties useSpark=%s\n" +
        "where a.c = b.c", useSpark);

        String expected =
        "1          |\n" +
        "----------------------\n" +
        "-2000000000000000000 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select min(a.b+b.b) from ts_bigint a, ts_bigint b --splice-properties useSpark=%s\n" +
        "where a.c = b.c group by b.d", useSpark);

        expected =
        "1          |\n" +
        "----------------------\n" +
        "-2000000000000000000 |\n" +
        "          0          |\n" +
        "          0          |\n" +
        "          0          |\n" +
        " 1000000000000000000 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select min(a.b+b.b) from ts_bigint a, ts_bigint b --splice-properties useSpark=%s\n" +
        "where a.c = b.c group by rollup(b.d)", useSpark);

        expected =
        "1          |\n" +
        "----------------------\n" +
        "-2000000000000000000 |\n" +
        "-2000000000000000000 |\n" +
        "          0          |\n" +
        "          0          |\n" +
        "          0          |\n" +
        " 1000000000000000000 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select min(a.b+b.b) from ts_bigint a, ts_bigint b --splice-properties useSpark=%s\n" +
        "where a.c = b.c group by rollup(a.d, b.d)", useSpark);

        expected =
        "1          |\n" +
        "----------------------\n" +
        "-2000000000000000000 |\n" +
        "-2000000000000000000 |\n" +
        "-2000000000000000000 |\n" +
        "          0          |\n" +
        "          0          |\n" +
        "          0          |\n" +
        "          0          |\n" +
        "          0          |\n" +
        "          0          |\n" +
        "          0          |\n" +
        "          0          |\n" +
        "          0          |\n" +
        " 1000000000000000000 |\n" +
        " 1000000000000000000 |\n" +
        " 1000000000000000000 |\n" +
        " 2000000000000000000 |\n" +
        " 2000000000000000000 |\n" +
        " 2000000000000000000 |\n" +
        " 2000000000000000000 |\n" +
        " 2000000000000000000 |\n" +
        " 2000000000000000000 |\n" +
        " 2000000000000000000 |\n" +
        " 2000000000000000000 |\n" +
        " 2000000000000000000 |\n" +
        " 3000000000000000000 |\n" +
        " 3000000000000000000 |\n" +
        " 3000000000000000000 |\n" +
        " 3000000000000000000 |\n" +
        " 3000000000000000000 |\n" +
        " 3000000000000000000 |\n" +
        " 4000000000000000000 |";

        testQuery(sqlText, expected, methodWatcher);
    }

    @Test
    public void testNativeSparkJoinsWithSum() throws Exception {
        String sqlText = format("select sum(a.b+a.b) from ts_bigint a, ts_bigint b --splice-properties useSpark=%s\n" +
        "where a.c = b.c", useSpark);

        String expected =
        "1            |\n" +
        "-------------------------\n" +
        "10240000000000000000000 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select sum(a.b+a.b) from ts_bigint a, ts_bigint b --splice-properties useSpark=%s\n" +
        "where a.c = b.c group by b.d", useSpark);

        expected =
        "1           |\n" +
        "------------------------\n" +
        "2048000000000000000000 |\n" +
        "2048000000000000000000 |\n" +
        "2048000000000000000000 |\n" +
        "2048000000000000000000 |\n" +
        "2048000000000000000000 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select sum(a.b+a.b) from ts_bigint a, ts_bigint b --splice-properties useSpark=%s\n" +
        "where a.c = b.c group by rollup(b.d)", useSpark);

        expected =
        "1            |\n" +
        "-------------------------\n" +
        "10240000000000000000000 |\n" +
        "2048000000000000000000  |\n" +
        "2048000000000000000000  |\n" +
        "2048000000000000000000  |\n" +
        "2048000000000000000000  |\n" +
        "2048000000000000000000  |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select sum(a.b+a.b) from ts_bigint a, ts_bigint b --splice-properties useSpark=%s\n" +
        "where a.c = b.c group by rollup(a.d, b.d)", useSpark);

        expected =
        "1            |\n" +
        "-------------------------\n" +
        "-2560000000000000000000 |\n" +
        "-512000000000000000000  |\n" +
        "-512000000000000000000  |\n" +
        "-512000000000000000000  |\n" +
        "-512000000000000000000  |\n" +
        "-512000000000000000000  |\n" +
        "1024000000000000000000  |\n" +
        "1024000000000000000000  |\n" +
        "1024000000000000000000  |\n" +
        "1024000000000000000000  |\n" +
        "1024000000000000000000  |\n" +
        "10240000000000000000000 |\n" +
        "2560000000000000000000  |\n" +
        "2560000000000000000000  |\n" +
        "2560000000000000000000  |\n" +
        " 512000000000000000000  |\n" +
        " 512000000000000000000  |\n" +
        " 512000000000000000000  |\n" +
        " 512000000000000000000  |\n" +
        " 512000000000000000000  |\n" +
        " 512000000000000000000  |\n" +
        " 512000000000000000000  |\n" +
        " 512000000000000000000  |\n" +
        " 512000000000000000000  |\n" +
        " 512000000000000000000  |\n" +
        " 512000000000000000000  |\n" +
        " 512000000000000000000  |\n" +
        " 512000000000000000000  |\n" +
        " 512000000000000000000  |\n" +
        " 512000000000000000000  |\n" +
        "5120000000000000000000  |";

        testQuery(sqlText, expected, methodWatcher);
    }


    @Test
    public void testNativeSparkJoinsWithSumDistinct() throws Exception {
        String sqlText = format("select sum(distinct a.b+b.b) from ts_bigint a, ts_bigint b --splice-properties useSpark=%s\n" +
        "where a.c = b.c", useSpark);

        String expected =
        "1          |\n" +
        "---------------------\n" +
        "8000000000000000000 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select sum(distinct a.b+b.b) from ts_bigint a, ts_bigint b --splice-properties useSpark=%s\n" +
        "where a.c = b.c group by b.d", useSpark);

        expected =
        "1          |\n" +
        "----------------------\n" +
        "-1000000000000000000 |\n" +
        " 5000000000000000000 |\n" +
        " 5000000000000000000 |\n" +
        " 5000000000000000000 |\n" +
        " 8000000000000000000 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select sum(distinct a.b+b.b) from ts_bigint a, ts_bigint b --splice-properties useSpark=%s\n" +
        "where a.c = b.c group by rollup(b.d)", useSpark);

        expected =
        "1          |\n" +
        "----------------------\n" +
        "-1000000000000000000 |\n" +
        " 5000000000000000000 |\n" +
        " 5000000000000000000 |\n" +
        " 5000000000000000000 |\n" +
        " 8000000000000000000 |\n" +
        " 8000000000000000000 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select sum(distinct a.b+b.b) from ts_bigint a, ts_bigint b --splice-properties useSpark=%s\n" +
        "where a.c = b.c group by rollup(a.d, b.d)", useSpark);

        expected =
        "1          |\n" +
        "----------------------\n" +
        "-1000000000000000000 |\n" +
        "-2000000000000000000 |\n" +
        "          0          |\n" +
        "          0          |\n" +
        "          0          |\n" +
        "          0          |\n" +
        "          0          |\n" +
        "          0          |\n" +
        " 1000000000000000000 |\n" +
        " 1000000000000000000 |\n" +
        " 2000000000000000000 |\n" +
        " 2000000000000000000 |\n" +
        " 2000000000000000000 |\n" +
        " 2000000000000000000 |\n" +
        " 2000000000000000000 |\n" +
        " 2000000000000000000 |\n" +
        " 2000000000000000000 |\n" +
        " 2000000000000000000 |\n" +
        " 2000000000000000000 |\n" +
        " 3000000000000000000 |\n" +
        " 3000000000000000000 |\n" +
        " 3000000000000000000 |\n" +
        " 3000000000000000000 |\n" +
        " 3000000000000000000 |\n" +
        " 3000000000000000000 |\n" +
        " 4000000000000000000 |\n" +
        " 5000000000000000000 |\n" +
        " 5000000000000000000 |\n" +
        " 5000000000000000000 |\n" +
        " 8000000000000000000 |\n" +
        " 8000000000000000000 |";

        testQuery(sqlText, expected, methodWatcher);

    }

    @Test
    public void testNativeSparkJoinsWithAvg() throws Exception {
        String sqlText = format("select avg(a.b+b.b) from ts_bigint a, ts_bigint b --splice-properties useSpark=%s\n" +
        "where a.c = b.c", useSpark);

        String expected =
        "1            |\n" +
        "--------------------------\n" +
        "1600000000000000000.0000 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select avg(a.b+b.b) from ts_bigint a, ts_bigint b --splice-properties useSpark=%s\n" +
        "where a.c = b.c group by b.d", useSpark);

        expected =
        "1            |\n" +
        "--------------------------\n" +
        "-200000000000000000.0000 |\n" +
        "1800000000000000000.0000 |\n" +
        "1800000000000000000.0000 |\n" +
        "1800000000000000000.0000 |\n" +
        "2800000000000000000.0000 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select avg(a.b+b.b) from ts_bigint a, ts_bigint b --splice-properties useSpark=%s\n" +
        "where a.c = b.c group by rollup(b.d)", useSpark);

        expected =
        "1            |\n" +
        "--------------------------\n" +
        "-200000000000000000.0000 |\n" +
        "1600000000000000000.0000 |\n" +
        "1800000000000000000.0000 |\n" +
        "1800000000000000000.0000 |\n" +
        "1800000000000000000.0000 |\n" +
        "2800000000000000000.0000 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select avg(a.b+b.b) from ts_bigint a, ts_bigint b --splice-properties useSpark=%s\n" +
        "where a.c = b.c group by rollup(a.d, b.d)", useSpark);

        expected =
        "1             |\n" +
        "---------------------------\n" +
        "-200000000000000000.0000  |\n" +
        "-2000000000000000000.0000 |\n" +
        "         0.0000           |\n" +
        "         0.0000           |\n" +
        "         0.0000           |\n" +
        "         0.0000           |\n" +
        "         0.0000           |\n" +
        "         0.0000           |\n" +
        "1000000000000000000.0000  |\n" +
        "1000000000000000000.0000  |\n" +
        "1600000000000000000.0000  |\n" +
        "1800000000000000000.0000  |\n" +
        "1800000000000000000.0000  |\n" +
        "1800000000000000000.0000  |\n" +
        "2000000000000000000.0000  |\n" +
        "2000000000000000000.0000  |\n" +
        "2000000000000000000.0000  |\n" +
        "2000000000000000000.0000  |\n" +
        "2000000000000000000.0000  |\n" +
        "2000000000000000000.0000  |\n" +
        "2000000000000000000.0000  |\n" +
        "2000000000000000000.0000  |\n" +
        "2000000000000000000.0000  |\n" +
        "2800000000000000000.0000  |\n" +
        "3000000000000000000.0000  |\n" +
        "3000000000000000000.0000  |\n" +
        "3000000000000000000.0000  |\n" +
        "3000000000000000000.0000  |\n" +
        "3000000000000000000.0000  |\n" +
        "3000000000000000000.0000  |\n" +
        "4000000000000000000.0000  |";

        testQuery(sqlText, expected, methodWatcher);
    }

    @Test
    public void testNativeSparkJoinsWithAvgDistinct() throws Exception {
        String sqlText = format("select avg(distinct a.b+b.b) from ts_bigint a, ts_bigint b --splice-properties useSpark=%s\n" +
        "where a.c = b.c", useSpark);

        String expected =
        "1            |\n" +
        "--------------------------\n" +
        "1333333333333333333.3333 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select avg(distinct a.b+b.b) from ts_bigint a, ts_bigint b --splice-properties useSpark=%s\n" +
        "where a.c = b.c group by b.d", useSpark);

        expected =
        "1            |\n" +
        "--------------------------\n" +
        "-333333333333333333.3333 |\n" +
        "1666666666666666666.6667 |\n" +
        "1666666666666666666.6667 |\n" +
        "1666666666666666666.6667 |\n" +
        "2666666666666666666.6667 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select avg(distinct a.b+b.b) from ts_bigint a, ts_bigint b --splice-properties useSpark=%s\n" +
        "where a.c = b.c group by rollup(b.d)", useSpark);

        expected =
        "1            |\n" +
        "--------------------------\n" +
        "-333333333333333333.3333 |\n" +
        "1333333333333333333.3333 |\n" +
        "1666666666666666666.6667 |\n" +
        "1666666666666666666.6667 |\n" +
        "1666666666666666666.6667 |\n" +
        "2666666666666666666.6667 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select avg(distinct a.b+b.b) from ts_bigint a, ts_bigint b --splice-properties useSpark=%s\n" +
        "where a.c = b.c group by rollup(a.d, b.d)", useSpark);

        expected =
        "1             |\n" +
        "---------------------------\n" +
        "-2000000000000000000.0000 |\n" +
        "-333333333333333333.3333  |\n" +
        "         0.0000           |\n" +
        "         0.0000           |\n" +
        "         0.0000           |\n" +
        "         0.0000           |\n" +
        "         0.0000           |\n" +
        "         0.0000           |\n" +
        "1000000000000000000.0000  |\n" +
        "1000000000000000000.0000  |\n" +
        "1333333333333333333.3333  |\n" +
        "1666666666666666666.6667  |\n" +
        "1666666666666666666.6667  |\n" +
        "1666666666666666666.6667  |\n" +
        "2000000000000000000.0000  |\n" +
        "2000000000000000000.0000  |\n" +
        "2000000000000000000.0000  |\n" +
        "2000000000000000000.0000  |\n" +
        "2000000000000000000.0000  |\n" +
        "2000000000000000000.0000  |\n" +
        "2000000000000000000.0000  |\n" +
        "2000000000000000000.0000  |\n" +
        "2000000000000000000.0000  |\n" +
        "2666666666666666666.6667  |\n" +
        "3000000000000000000.0000  |\n" +
        "3000000000000000000.0000  |\n" +
        "3000000000000000000.0000  |\n" +
        "3000000000000000000.0000  |\n" +
        "3000000000000000000.0000  |\n" +
        "3000000000000000000.0000  |\n" +
        "4000000000000000000.0000  |";

        testQuery(sqlText, expected, methodWatcher);
    }

    @Test
    public void testNativeSparkJoinsWithCount() throws Exception {
        String sqlText = format("select count(a.b+b.b) from ts_bigint a, ts_bigint b --splice-properties useSpark=%s\n" +
        "where a.c = b.c", useSpark);

        String expected =
        "1  |\n" +
        "------\n" +
        "6400 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select count(a.b+b.b) from ts_bigint a, ts_bigint b --splice-properties useSpark=%s\n" +
        "where a.c = b.c group by b.d", useSpark);

        expected =
        "1  |\n" +
        "------\n" +
        "1280 |\n" +
        "1280 |\n" +
        "1280 |\n" +
        "1280 |\n" +
        "1280 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select count(a.b+b.b) from ts_bigint a, ts_bigint b --splice-properties useSpark=%s\n" +
        "where a.c = b.c group by rollup(b.d)", useSpark);

        expected =
        "1  |\n" +
        "------\n" +
        "1280 |\n" +
        "1280 |\n" +
        "1280 |\n" +
        "1280 |\n" +
        "1280 |\n" +
        "6400 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select count(a.b+b.b) from ts_bigint a, ts_bigint b --splice-properties useSpark=%s\n" +
        "where a.c = b.c group by rollup(a.d, b.d)", useSpark);

        expected =
        "1  |\n" +
        "------\n" +
        "1280 |\n" +
        "1280 |\n" +
        "1280 |\n" +
        "1280 |\n" +
        "1280 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        "6400 |";

        testQuery(sqlText, expected, methodWatcher);
    }

    @Test
    public void testNativeSparkJoinsWithCountStar() throws Exception {
        String sqlText = format("select count(*) from ts_bigint a, ts_bigint b --splice-properties useSpark=%s\n" +
        "where a.c = b.c", useSpark);

        String expected =
        "1  |\n" +
        "------\n" +
        "6400 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select count(*) from ts_bigint a, ts_bigint b --splice-properties useSpark=%s\n" +
        "where a.c = b.c group by b.d", useSpark);

        expected =
        "1  |\n" +
        "------\n" +
        "1280 |\n" +
        "1280 |\n" +
        "1280 |\n" +
        "1280 |\n" +
        "1280 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select count(*) from ts_bigint a, ts_bigint b --splice-properties useSpark=%s\n" +
        "where a.c = b.c group by rollup(b.d)", useSpark);

        expected =
        "1  |\n" +
        "------\n" +
        "1280 |\n" +
        "1280 |\n" +
        "1280 |\n" +
        "1280 |\n" +
        "1280 |\n" +
        "6400 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select count(*) from ts_bigint a, ts_bigint b --splice-properties useSpark=%s\n" +
        "where a.c = b.c group by rollup(a.d, b.d)", useSpark);

        expected =
        "1  |\n" +
        "------\n" +
        "1280 |\n" +
        "1280 |\n" +
        "1280 |\n" +
        "1280 |\n" +
        "1280 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        " 256 |\n" +
        "6400 |";

        testQuery(sqlText, expected, methodWatcher);
    }

    @Test
    public void testNativeSparkJoinsWithCountDistinct() throws Exception {
        String sqlText = format("select count(distinct a.b+b.b), count(distinct a.b), count(distinct b.b), count(distinct b.c) from ts_bigint a, ts_bigint b --splice-properties useSpark=%s\n" +
        "where a.c = b.c", useSpark);

        String expected =
        "1 | 2 | 3 | 4 |\n" +
        "----------------\n" +
        " 6 | 3 | 3 | 1 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select count(distinct a.b+b.b), count(distinct a.b), count(distinct b.b), count(distinct b.c) from ts_bigint a, ts_bigint b --splice-properties useSpark=%s\n" +
        "where a.c = b.c group by b.d", useSpark);

        expected =
        "1 | 2 | 3 | 4 |\n" +
        "----------------\n" +
        " 3 | 3 | 1 | 1 |\n" +
        " 3 | 3 | 1 | 1 |\n" +
        " 3 | 3 | 1 | 1 |\n" +
        " 3 | 3 | 1 | 1 |\n" +
        " 3 | 3 | 1 | 1 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select count(distinct a.b+b.b), count(distinct a.b), count(distinct b.b), count(distinct b.c) from ts_bigint a, ts_bigint b --splice-properties useSpark=%s\n" +
        "where a.c = b.c group by rollup(b.d)", useSpark);

        expected =
        "1 | 2 | 3 | 4 |\n" +
        "----------------\n" +
        " 3 | 3 | 1 | 1 |\n" +
        " 3 | 3 | 1 | 1 |\n" +
        " 3 | 3 | 1 | 1 |\n" +
        " 3 | 3 | 1 | 1 |\n" +
        " 3 | 3 | 1 | 1 |\n" +
        " 6 | 3 | 3 | 1 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select count(distinct a.b+b.b), count(distinct a.b), count(distinct b.b), count(distinct b.c) from ts_bigint a, ts_bigint b --splice-properties useSpark=%s\n" +
        "where a.c = b.c group by rollup(a.d, b.d)", useSpark);

        expected =
        "1 | 2 | 3 | 4 |\n" +
        "----------------\n" +
        " 1 | 1 | 1 | 1 |\n" +
        " 1 | 1 | 1 | 1 |\n" +
        " 1 | 1 | 1 | 1 |\n" +
        " 1 | 1 | 1 | 1 |\n" +
        " 1 | 1 | 1 | 1 |\n" +
        " 1 | 1 | 1 | 1 |\n" +
        " 1 | 1 | 1 | 1 |\n" +
        " 1 | 1 | 1 | 1 |\n" +
        " 1 | 1 | 1 | 1 |\n" +
        " 1 | 1 | 1 | 1 |\n" +
        " 1 | 1 | 1 | 1 |\n" +
        " 1 | 1 | 1 | 1 |\n" +
        " 1 | 1 | 1 | 1 |\n" +
        " 1 | 1 | 1 | 1 |\n" +
        " 1 | 1 | 1 | 1 |\n" +
        " 1 | 1 | 1 | 1 |\n" +
        " 1 | 1 | 1 | 1 |\n" +
        " 1 | 1 | 1 | 1 |\n" +
        " 1 | 1 | 1 | 1 |\n" +
        " 1 | 1 | 1 | 1 |\n" +
        " 1 | 1 | 1 | 1 |\n" +
        " 1 | 1 | 1 | 1 |\n" +
        " 1 | 1 | 1 | 1 |\n" +
        " 1 | 1 | 1 | 1 |\n" +
        " 1 | 1 | 1 | 1 |\n" +
        " 3 | 1 | 3 | 1 |\n" +
        " 3 | 1 | 3 | 1 |\n" +
        " 3 | 1 | 3 | 1 |\n" +
        " 3 | 1 | 3 | 1 |\n" +
        " 3 | 1 | 3 | 1 |\n" +
        " 6 | 3 | 3 | 1 |";

        testQuery(sqlText, expected, methodWatcher);
    }

    @Test
    public void testNativeSparkJoinsWithStddev_pop() throws Exception {
        String sqlText = format("select cast(stddev_pop((a.b+b.b)/100000000000000) as decimal(38,9)), cast(stddev_pop(a.b/100000000000000) as decimal(38,9)), cast(stddev_pop(b.b/100000000000000) as decimal(38,9)), cast(stddev_pop(b.c) as decimal(38,9)) from ts_bigint a, ts_bigint b --splice-properties useSpark=%s\n" +
        "where a.c = b.c", useSpark);

        String expected =
            "1        |       2       |       3       |  4  |\n" +
            "-------------------------------------------------------\n" +
            "13856.406460551 |9797.958971133 |9797.958971133 |0E-9 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select cast(stddev_pop((a.b+b.b)/100000000000000) as decimal(38,9)), cast(stddev_pop(a.b/100000000000000) as decimal(38,9)), cast(stddev_pop(b.b/100000000000000) as decimal(38,9)), cast(stddev_pop(b.c) as decimal(38,9)) from ts_bigint a, ts_bigint b --splice-properties useSpark=%s\n" +
        "where a.c = b.c group by b.d", useSpark);

        expected =
            "1       |       2       |  3  |  4  |\n" +
            "--------------------------------------------\n" +
            "9797.958971133 |9797.958971133 |0E-9 |0E-9 |\n" +
            "9797.958971133 |9797.958971133 |0E-9 |0E-9 |\n" +
            "9797.958971133 |9797.958971133 |0E-9 |0E-9 |\n" +
            "9797.958971133 |9797.958971133 |0E-9 |0E-9 |\n" +
            "9797.958971133 |9797.958971133 |0E-9 |0E-9 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select cast(stddev_pop((a.b+b.b)/100000000000000) as decimal(38,9)), cast(stddev_pop(a.b/100000000000000) as decimal(38,9)), cast(stddev_pop(b.b/100000000000000) as decimal(38,9)), cast(stddev_pop(b.c) as decimal(38,9)) from ts_bigint a, ts_bigint b --splice-properties useSpark=%s\n" +
        "where a.c = b.c group by rollup(b.d)", useSpark);

        expected =
            "1        |       2       |       3       |  4  |\n" +
            "-------------------------------------------------------\n" +
            "13856.406460551 |9797.958971133 |9797.958971133 |0E-9 |\n" +
            "9797.958971133  |9797.958971133 |     0E-9      |0E-9 |\n" +
            "9797.958971133  |9797.958971133 |     0E-9      |0E-9 |\n" +
            "9797.958971133  |9797.958971133 |     0E-9      |0E-9 |\n" +
            "9797.958971133  |9797.958971133 |     0E-9      |0E-9 |\n" +
            "9797.958971133  |9797.958971133 |     0E-9      |0E-9 |";

        testQuery(sqlText, expected, methodWatcher);

        sqlText = format("select cast(stddev_pop((a.b+b.b)/100000000000000) as decimal(38,9)), cast(stddev_pop(a.b/100000000000000) as decimal(38,9)), cast(stddev_pop(b.b/100000000000000) as decimal(38,9)), cast(stddev_pop(b.c) as decimal(38,9)) from ts_bigint a, ts_bigint b --splice-properties useSpark=%s\n" +
        "where a.c = b.c group by rollup(a.d, b.d)", useSpark);

        expected =
            "1        |       2       |       3       |  4  |\n" +
            "-------------------------------------------------------\n" +
            "     0E-9       |     0E-9      |     0E-9      |0E-9 |\n" +
            "     0E-9       |     0E-9      |     0E-9      |0E-9 |\n" +
            "     0E-9       |     0E-9      |     0E-9      |0E-9 |\n" +
            "     0E-9       |     0E-9      |     0E-9      |0E-9 |\n" +
            "     0E-9       |     0E-9      |     0E-9      |0E-9 |\n" +
            "     0E-9       |     0E-9      |     0E-9      |0E-9 |\n" +
            "     0E-9       |     0E-9      |     0E-9      |0E-9 |\n" +
            "     0E-9       |     0E-9      |     0E-9      |0E-9 |\n" +
            "     0E-9       |     0E-9      |     0E-9      |0E-9 |\n" +
            "     0E-9       |     0E-9      |     0E-9      |0E-9 |\n" +
            "     0E-9       |     0E-9      |     0E-9      |0E-9 |\n" +
            "     0E-9       |     0E-9      |     0E-9      |0E-9 |\n" +
            "     0E-9       |     0E-9      |     0E-9      |0E-9 |\n" +
            "     0E-9       |     0E-9      |     0E-9      |0E-9 |\n" +
            "     0E-9       |     0E-9      |     0E-9      |0E-9 |\n" +
            "     0E-9       |     0E-9      |     0E-9      |0E-9 |\n" +
            "     0E-9       |     0E-9      |     0E-9      |0E-9 |\n" +
            "     0E-9       |     0E-9      |     0E-9      |0E-9 |\n" +
            "     0E-9       |     0E-9      |     0E-9      |0E-9 |\n" +
            "     0E-9       |     0E-9      |     0E-9      |0E-9 |\n" +
            "     0E-9       |     0E-9      |     0E-9      |0E-9 |\n" +
            "     0E-9       |     0E-9      |     0E-9      |0E-9 |\n" +
            "     0E-9       |     0E-9      |     0E-9      |0E-9 |\n" +
            "     0E-9       |     0E-9      |     0E-9      |0E-9 |\n" +
            "     0E-9       |     0E-9      |     0E-9      |0E-9 |\n" +
            "13856.406460551 |9797.958971133 |9797.958971133 |0E-9 |\n" +
            "9797.958971133  |     0E-9      |9797.958971133 |0E-9 |\n" +
            "9797.958971133  |     0E-9      |9797.958971133 |0E-9 |\n" +
            "9797.958971133  |     0E-9      |9797.958971133 |0E-9 |\n" +
            "9797.958971133  |     0E-9      |9797.958971133 |0E-9 |\n" +
            "9797.958971133  |     0E-9      |9797.958971133 |0E-9 |";

        testQuery(sqlText, expected, methodWatcher);
    }
    //SUM, MAX, MIN, AVG, COUNT, COUNT, STDDEV_POP, COUNT(DISTINCT ), SUM(DISTINCT ), AVG(DISTINCT )
}
