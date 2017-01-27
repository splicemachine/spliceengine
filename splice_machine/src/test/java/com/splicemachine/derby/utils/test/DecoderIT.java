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

package com.splicemachine.derby.utils.test;

import com.splicemachine.derby.test.framework.SpliceNetConnection;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.List;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

/**
 * Test for Bug DB-923.
 *
 * The old behavior of this bug was that row with transaction_header_key caused
 * "ERROR 22003: The resulting value is outside the range for the data type DOUBLE." but only
 * when the 20 row sample was imported.  When row 18 is imported into a table by itself,
 * the table (or row by itself) could be queried without error.  When stepping thru this in
 * the debugger, the bad Double that was being created was 1.04282087854243E-309.  This value
 * is not found in any of the demodata csv files. This value was the result of row decoding.
 *
 * Also, when column values were pulled from the csv file and Double.parseDouble(val) called,
 * all values behaved as expected.
 *
 * I didn't change any code to fix this behavior.  This test was failing when I created it but
 * I'm now at commit 8d6d889 and we no longer exhibit the behavior.  Some code change between
 * commit 293fbd6 and 8d6d889 fixed it.
 *
 * @author Jeff Cunningham
 *         Date: 1/6/14
 */
@Ignore("DB-4272")
public class DecoderIT {
    private static final Logger LOG = Logger.getLogger(DecoderIT.class);

    private static final String SCHEMA_NAME = DecoderIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    private static final String csvLocation = SpliceUnitTest.getResourceDirectory()+ "txn_header_20rows.csv";
    private static final String csvLocation2 = SpliceUnitTest.getResourceDirectory()+"txn_header_1row.csv";

    protected static TransactionHeaderTable headerTableWatcher = new TransactionHeaderTable(TransactionHeaderTable.TABLE_NAME,SCHEMA_NAME) {
        @Override
        protected void starting(Description description) {
            super.starting(description);
            try {
                PreparedStatement ps =
                        SpliceNetConnection.getConnection().prepareStatement(
                                String.format("call SYSCS_UTIL.IMPORT_DATA('%s','%s', null, '%s', ',', null, null,null,null,0,null,true,null)",
                                        SCHEMA_NAME, TransactionHeaderTable.TABLE_NAME, csvLocation));
                ResultSet rs = ps.executeQuery();
                while (rs.next()) {

                }
            } catch (Exception e) {
                LOG.error("Error inserting into "+TransactionHeaderTable.TABLE_NAME+" table", e);
                throw new RuntimeException(e);
            }
        }
    };

    protected static TransactionHeaderTable headerTableWatcher2 = new TransactionHeaderTable(TransactionHeaderTable.TABLE_NAME2,SCHEMA_NAME) {
        @Override
        protected void starting(Description description) {
            super.starting(description);
            try {
                PreparedStatement ps =
                        SpliceNetConnection.getConnection().prepareStatement(
                                String.format("call SYSCS_UTIL.IMPORT_DATA('%s','%s', null, '%s', ',', null, null,null,null,0,null,true,null)",
                                        SCHEMA_NAME, TransactionHeaderTable.TABLE_NAME2, csvLocation2));
                ResultSet rs = ps.executeQuery();
                while (rs.next()) {

                }
            } catch (Exception e) {
                LOG.error("Error inserting into "+TransactionHeaderTable.TABLE_NAME2+" table", e);
                throw new RuntimeException(e);
            }
        }
    };

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(headerTableWatcher)
            .around(headerTableWatcher2);

    @Test
    public void testQueryRow20RowTable() throws Exception {
        String query = String.format("select * from %s.%s where %s.%s.transaction_header_key = 18",
                SCHEMA_NAME, TransactionHeaderTable.TABLE_NAME,
                SCHEMA_NAME, TransactionHeaderTable.TABLE_NAME);
        ResultSet rs = methodWatcher.executeQuery(query);
        TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
//        System.out.println(fr.toString());
        Assert.assertEquals(1, fr.size());
    }

    @Test
    public void testQueryRow1RowTable() throws Exception {
        String query = String.format("select * from %s.%s where %s.%s.transaction_header_key = 18",
                SCHEMA_NAME, TransactionHeaderTable.TABLE_NAME2,
                SCHEMA_NAME, TransactionHeaderTable.TABLE_NAME2);
        ResultSet rs = methodWatcher.executeQuery(query);
        TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
//        System.out.println(fr.toString());
        Assert.assertEquals(1, fr.size());
    }

    @Test
    public void testQueryRowOne20RowTable() throws Exception {
        String query = String.format("select * from %s.%s where %s.%s.transaction_header_key = 1",
                SCHEMA_NAME, TransactionHeaderTable.TABLE_NAME,
                SCHEMA_NAME, TransactionHeaderTable.TABLE_NAME);
        ResultSet rs = methodWatcher.executeQuery(query);
        TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
//        System.out.println(fr.toString());
        Assert.assertEquals(1, fr.size());
    }

    @Test
    public void testQueryAllRows20RowTable() throws Exception {
        String query = String.format("select * from %s.%s",
                SCHEMA_NAME, TransactionHeaderTable.TABLE_NAME);
        ResultSet rs = methodWatcher.executeQuery(query);
        TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
//        System.out.println(fr.toString());
        Assert.assertEquals(20, fr.size());
    }

    @Test
    public void testQueryEachRow20RowTable() throws Exception {
        for (int i = 1; i<=20; ++i) {
            String query = String.format("select * from %s.%s where %s.%s.transaction_header_key = %d",
                    SCHEMA_NAME, TransactionHeaderTable.TABLE_NAME,
                    SCHEMA_NAME, TransactionHeaderTable.TABLE_NAME,
                    i);
            ResultSet rs = methodWatcher.executeQuery(query);
            TestUtils.FormattedResult fr = TestUtils.FormattedResult.ResultFactory.convert(query, rs);
//            System.out.println(fr.toString());
            Assert.assertEquals("Row was "+i,1, fr.size());
        }
    }

    private Iterable<String> readRows(String csvLocation) {
        List<String> lines = new LinkedList<String>();
        BufferedReader in = null;
        try {
            in = new BufferedReader(new FileReader(csvLocation));

            String line = in.readLine();
            while(line != null) {
                lines.add(line);
                line = in.readLine();
            }
        } catch (IOException e) {
            Assert.fail("Unable to read: " + csvLocation + ": " + e.getLocalizedMessage());
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
        return lines;
    }

    public static class TransactionHeaderTable extends SpliceTableWatcher {

        public static final String TABLE_NAME = "TXN_HEADER";
        public static final String TABLE_NAME2 = "TXN_HEADER2";

        public TransactionHeaderTable(String tableName, String schemaName) {
            super(tableName,schemaName,CREATE_STRING);
        }

        private static String CREATE_STRING =
                "(" +
                    "TRANSACTION_HEADER_KEY BIGINT NOT NULL, " +
                    "CUSTOMER_MASTER_ID BIGINT, " +
                    "HOUSEHOLD_MASTER_ID BIGINT NOT NULL, " +
                    "TRANSACTION_DT DATE NOT NULL, " +
                    "STORE_NBR SMALLINT NOT NULL, " +
                    "REGISTER_NBR INTEGER, " +
                    "TRANSACTION_NBR INTEGER, " +
                    "VISIT_KEY BIGINT, " +
                    "TRANSACTION_TIME_HHMISS VARCHAR(8), " +
                    "DIRECT_TRANSACTION_DT DATE, " +
                    "SOURCE_SALES_INSTANCE_ID BIGINT, " +
                    "SOURCE_ORDER_NBR BIGINT, " +
                    "SOURCE_CASHIER_NBR INTEGER, " +
                    "TRANSACTION_LINK_TYPE VARCHAR(3), " +
                    "GHOST_CUSTOMER_FLG VARCHAR(1), " +
                    "SALES_TYPE_CD SMALLINT, " +
                    "VOID_TYPE_CD VARCHAR(1), " +
                    "SALES_AMT DECIMAL(9,2), " +
                    "SALES_COST DECIMAL(9,2), " +
                    "SALES_QTY INTEGER, " +
                    "RETURN_AMT DECIMAL(9,2), " +
                    "RETURN_COST DECIMAL(9,2), " +
                    "RETURN_QTY DECIMAL(9,2), " +
                    "SPECIAL_SERVICE_AMT DECIMAL(9,2), " +
                    "SPECIAL_SERVICE_QTY INTEGER, " +
                    "NET_COUPON_AMT DECIMAL(9,2), " +
                    "NET_COUPON_QTY INTEGER, " +
                    "NET_SALES_AMT DECIMAL(9,2), " +
                    "NET_SALES_COST DECIMAL(9,2), " +
                    "NET_SALES_QTY INTEGER, " +
                    "NET_DISCOUNT_AMT DECIMAL(9,2), " +
                    "NET_DISCOUNT_QTY INTEGER, " +
                    "NET_SPECIAL_SALES_AMT DECIMAL(9,2), " +
                    "NET_SPECIAL_SALES_QTY INTEGER, " +
                    "MARGIN_AMT DECIMAL(9,2), " +
                    "MARGIN_DM_AMT DECIMAL(9,2), " +
                    "EXCHANGE_RATE_PERCENT DOUBLE PRECISION, " +
                    "POSITIVE_DETAIL_FLG VARCHAR(1), " +
                    "POSITIVE_HEADER_FLG VARCHAR(1), " +
                    "TRANSACTION_CRICKET_IND VARCHAR(1), " +
                    "ADOPTION_GROUP_ID VARCHAR(8), " +
                    "CREATE_USER_ID VARCHAR(30), " +
                    "CREATE_DT DATE, " +
                    "UPDATE_USER_ID VARCHAR(30), " +
                    "UPDATE_DT DATE, " +
                    "TRANSACTION_LINK_KEY VARCHAR(20), " +
                    "TOTAL_TP_SERVICE_COST DECIMAL(9,2), " +
                    "TOTAL_TP_DM_SERVICE_COST DECIMAL(9,2), " +
                    "GEOCAPTURE_FLG VARCHAR(1), " +
                    "NET_DDCSTAY_QTY INTEGER," +
                    "PRIMARY KEY(TRANSACTION_HEADER_KEY)" +
                    ")";
    }
}
