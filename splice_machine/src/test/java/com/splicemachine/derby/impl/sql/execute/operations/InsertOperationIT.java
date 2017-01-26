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

import org.spark_project.guava.collect.Maps;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.apache.commons.io.FileUtils;
import org.junit.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.*;
import java.util.*;

import static com.splicemachine.derby.test.framework.SpliceUnitTest.getBaseDirectory;
import static com.splicemachine.derby.test.framework.SpliceUnitTest.getResourceDirectory;

public class InsertOperationIT {

    private static final String SCHEMA = InsertOperationIT.class.getSimpleName();
    private static SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @BeforeClass
    public static void createSharedTables() throws Exception {
        classWatcher.executeUpdate("create table T (name varchar(40))");
        classWatcher.executeUpdate("create table S (name varchar(40))");
        classWatcher.executeUpdate("create table A (name varchar(40), count int)");

        classWatcher.executeUpdate("create table G (name varchar(40))");
        classWatcher.executeUpdate("create table B (name varchar(40))");
        classWatcher.executeUpdate("create table E (name varchar(40))");
        classWatcher.executeUpdate("create table J (name varchar(40))");
        classWatcher.executeUpdate("create table L (name varchar(40))");
        classWatcher.executeUpdate("create table Y (name varchar(40))");

        classWatcher.executeUpdate("create table Z (name varchar(40),count int)");
        classWatcher.executeUpdate("create table FILES (name varchar(32) not null primary key, doc blob(50M))");
        classWatcher.executeUpdate("create table HMM (b16a char(2) for bit data, b16b char(2) for bit data, vb16a varchar(2) for bit data, vb16b varchar(2) for bit data, lbv long varchar for bit data)");
        classWatcher.executeUpdate("create table WARNING (a char(1))");

        classWatcher.executeUpdate("create table T1 (c1 int generated always as identity, c2 int)");
        classWatcher.executeUpdate("create table T2 (a int, b int)");

        classWatcher.executeUpdate("create table T3 (a int, b decimal(16,10))");
        classWatcher.executeUpdate("insert into T3 values (1,1)");

        classWatcher.executeUpdate("create table T4 (c int, d int)");
        classWatcher.executeUpdate("insert into T4 values (1,1),(2,2)");

        classWatcher.executeUpdate("create table T5 (a int, c int,b decimal(16,10), d int)");
        classWatcher.executeUpdate("create table SAME_LENGTH (name varchar(40))");
        classWatcher.executeUpdate("create table batch_test (col1 int, col2 int, col3 int, primary key (col1))");
        classWatcher.executeUpdate("create table T6 (a int)");
        classWatcher.executeUpdate("create table T7 (name varchar(20))");

        classWatcher.executeUpdate("create table TABLE_DECIMAL (CUSTOMER_ID DECIMAL (10,0))");
        classWatcher.executeUpdate("create table TABLE_BIGINT (CUSTOMER_ID BIGINT)");
        classWatcher.executeUpdate("create table TABLE_RESULT (CUSTOMER_ID BIGINT)");
        classWatcher.executeUpdate("insert into TABLE_BIGINT values (1),(2),(3)");
    }

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @Test
    public void testInsertOverMergeSortOuterJoinIsCorrect() throws Exception {
        /*
         * Regression test for DB-1833. Tests that we can insert over a subselect that has a merge-sort
         * join present,without getting any errors.
         */
        long insertCount = methodWatcher.executeUpdate(String.format("insert into %1$s select " +
                "%2$s.a,%3$s.c,%2$s.b,%3$s.d " +
                "from %2$s --SPLICE-PROPERTIES joinStrategy=SORTMERGE \n" +
                "right join %3$s on %2$s.a=%3$s.c", "T5", "T3", "T4"));
        Assert.assertEquals("Incorrect number of rows inserted!", 2, insertCount);
        ResultSet rs = methodWatcher.executeQuery("select * from T5");
        int count = 0;
        while (rs.next()) {
            int a = rs.getInt(1);
            if (rs.wasNull()) {
                BigDecimal b = rs.getBigDecimal(3);
                Assert.assertTrue("B is not null!", rs.wasNull());
            } else {
                BigDecimal b = rs.getBigDecimal(3);
                Assert.assertFalse("B is null!", rs.wasNull());
                Assert.assertTrue("Incorrect B value!", BigDecimal.ONE.subtract(b).abs().compareTo(new BigDecimal(".0000000001")) < 0);
            }
            count++;
            int c = rs.getInt(2);
            Assert.assertFalse("C is null!", rs.wasNull());
            int d = rs.getInt(4);
            Assert.assertFalse("D is null!", rs.wasNull());
        }
        Assert.assertEquals("Incorrect row count!", 2, count);
    }

    @Test
    public void testDataTruncationWarningIsEmitted() throws Exception {
        PreparedStatement ps = methodWatcher.prepareStatement("insert into WARNING values cast(? as char(1))");
        ps.setString(1, "12");
        int updated = ps.executeUpdate();
        Assert.assertEquals("Incorrect number of rows updated!", 1, updated);

        SQLWarning warning = ps.getWarnings();
        String sqlState = warning.getSQLState();
        Assert.assertEquals("Incorrect warning code returned!", "01004", sqlState);
    }

    @Test
    public void testInsertMultipleRecordsWithSameLength() throws Exception {
                /*Regression test for DB-1278*/
        Statement s = methodWatcher.getStatement();
        s.execute("insert into SAME_LENGTH (name) values ('ab'),('de'),('fg')");
        List<String> correctNames = Arrays.asList("ab", "de", "fg");
        Collections.sort(correctNames);
        ResultSet rs = methodWatcher.executeQuery("select * from SAME_LENGTH");
        List<String> names = new ArrayList<>();
        while (rs.next()) {
            names.add(rs.getString(1));
        }
        Collections.sort(names);
        Assert.assertEquals("returned named incorrect!", correctNames, names);
    }

    @Test
    public void testInsertMultipleRecords() throws Exception {
        Statement s = methodWatcher.getStatement();
        s.execute("insert into T(name) values ('gdavis'),('mzweben'),('rreimer')");
        List<String> correctNames = Arrays.asList("gdavis", "mzweben", "rreimer");
        Collections.sort(correctNames);
        ResultSet rs = methodWatcher.executeQuery("select * from T");
        List<String> names = new ArrayList<>();
        while (rs.next()) {
            names.add(rs.getString(1));
        }
        Collections.sort(names);
        Assert.assertEquals("returned named incorrect!", correctNames, names);
    }

    @Test
    public void testInsertSingleRecord() throws Exception {
        Statement s = methodWatcher.getStatement();
        s.execute("insert into S (name) values ('gdavis')");
        ResultSet rs = methodWatcher.executeQuery("select * from S");
        int count = 0;
        while (rs.next()) {
            count++;
            Assert.assertNotNull(rs.getString(1));
        }
        Assert.assertEquals("Incorrect Number of Results Returned", 1, count);
    }

    @Test
    public void testInsertFromSubselect() throws Exception {
        Statement s = methodWatcher.getStatement();
        s.execute("insert into G values('sfines')");
        s.execute("insert into G values('jzhang')");
        s.execute("insert into G values('jleach')");
        methodWatcher.commit();
        List<String> correctNames = Arrays.asList("sfines", "jzhang", "jleach");
        Collections.sort(correctNames);
        //copy that data into table t
        s = methodWatcher.getStatement();
        s.execute("insert into B (name) select name from G");
        methodWatcher.commit();
        ResultSet rs = methodWatcher.executeQuery("select * from B");
        List<String> names = new ArrayList<>();
        while (rs.next()) {
            names.add(rs.getString(1));
        }
        Collections.sort(names);
        Assert.assertEquals("returned named incorrect!", correctNames, names);
        methodWatcher.commit();
    }

    @Test
    public void testInsertVarBit() throws Exception {
        methodWatcher.executeUpdate("insert into HMM values(X'11', X'22', X'33', X'44', X'55')");
    }


    @Test
    public void testInsertReportsCorrectReturnedNumber() throws Exception {
        PreparedStatement ps = methodWatcher.prepareStatement("insert into E (name) values (?)");
        ps.setString(1, "bob");
        int returned = ps.executeUpdate();
        Assert.assertEquals("incorrect update count returned!", 1, returned);
    }

    /**
     * The idea here is to test that PreparedStatement inserts won't barf if you do
     * multiple inserts with different where clauses each time
     *
     * @throws Exception
     */
    @Test
    public void testInsertFromBoundedSubSelectThatChanges() throws Exception {
        Statement s = methodWatcher.getStatement();
        s.execute("insert into L (name) values ('gdavis'),('mzweben'),('rreimer')");
        PreparedStatement ps = methodWatcher.prepareStatement("insert into J (name) select name from L a where a.name = ?");
        ps.setString(1, "rreimer");
        ps.executeUpdate();

        ResultSet rs = methodWatcher.executeQuery("select * from J");
        int count = 0;
        while (rs.next()) {
            Assert.assertEquals("Incorrect name inserted!", "rreimer", rs.getString(1));
            count++;
        }
        Assert.assertEquals("Incorrect number of results returned!", 1, count);
        ps.setString(1, "mzweben");
        ps.executeUpdate();
        List<String> correct = Arrays.asList("rreimer", "mzweben");
        rs = methodWatcher.executeQuery("select * from J");
        count = 0;
        while (rs.next()) {
            String next = rs.getString(1);
            boolean found = false;
            for (String correctName : correct) {
                if (correctName.equals(next)) {
                    found = true;
                    break;
                }
            }
            Assert.assertTrue("Value " + next + " unexpectedly appeared!", found);
            count++;
        }
        Assert.assertEquals("Incorrect number of results returned!", correct.size(), count);
    }

    @Test
    public void testInsertFromSubOperation() throws Exception {
        Map<String, Integer> nameCountMap = Maps.newHashMap();
        Statement s = methodWatcher.getStatement();
        s.execute("insert into Y  values('sfines')");
        s.execute("insert into Y values('sfines')");
        nameCountMap.put("sfines", 2);
        s.execute("insert into Y values('jzhang')");
        s.execute("insert into Y values('jzhang')");
        s.execute("insert into Y values('jzhang')");
        nameCountMap.put("jzhang", 3);
        s.execute("insert into Y values('jleach')");
        nameCountMap.put("jleach", 1);
        methodWatcher.commit();
        s = methodWatcher.getStatement();
        int rowsInserted = s.executeUpdate("insert into Z (name,count) select name,count(name) from Y group by name");
        Assert.assertEquals(nameCountMap.size(), rowsInserted);
        methodWatcher.commit();
        ResultSet rs = methodWatcher.executeQuery("select * from Z");
        int groupCount = 0;
        while (rs.next()) {
            String name = rs.getString(1);
            Integer count = rs.getInt(2);
            Assert.assertNotNull("Name is null!", name);
            Assert.assertNotNull("Count is null!", count);
            int correctCount = nameCountMap.get(name);
            Assert.assertEquals("Incorrect count returned for name " + name, correctCount, count.intValue());
            groupCount++;
        }
        Assert.assertEquals("Incorrect number of groups returned!", nameCountMap.size(), groupCount);
    }

    @Test
    public void testInsertBlob() throws Exception {
        InputStream fin = new FileInputStream(getResourceDirectory() + "order_line_500K.csv");
        PreparedStatement ps = methodWatcher.prepareStatement("insert into FILES (name, doc) values (?,?)");
        ps.setString(1, "csv_file");
        ps.setBinaryStream(2, fin);
        ps.execute();
        ResultSet rs = methodWatcher.executeQuery("SELECT doc FROM FILES WHERE name = 'csv_file'");
        byte buff[] = new byte[1024];
        while (rs.next()) {
            Blob ablob = rs.getBlob(1);
            File newFile = new File(getBaseDirectory() + "/target/order_line_500K.csv");
            if (newFile.exists()) {
                newFile.delete();
            }
            newFile.createNewFile();
            InputStream is = ablob.getBinaryStream();
            FileOutputStream fos = new FileOutputStream(newFile);
            for (int b = is.read(buff); b != -1; b = is.read(buff)) {
                fos.write(buff, 0, b);
            }
            is.close();
            fos.close();
        }
        File file1 = new File(getResourceDirectory() + "order_line_500K.csv");
        File file2 = new File(getBaseDirectory() + "/target/order_line_500K.csv");
        Assert.assertTrue("The files contents are not equivalent", FileUtils.contentEquals(file1, file2));
    }

    @Test
    public void testInsertIdentitySingleAndFromSelfScan() throws Exception {
        methodWatcher.executeUpdate("insert into T1(c2) values (1)");
        methodWatcher.executeUpdate("insert into T1(c2) values (1)");
        methodWatcher.executeUpdate("insert into T1 (c2) select c1 from T1");
        ResultSet rs = methodWatcher.executeQuery("select c1, c2 from T1");
        int i = 0;
        while (rs.next()) {
            i++;
        //    System.out.println("rs -> " + rs.getInt(1));
            Assert.assertTrue("These numbers should be contiguous", rs.getInt(1) >= 1 && rs.getInt(1) <= 4);
        }
        Assert.assertEquals("Should have returned 4 rows from identity insert", 4, i);
    }

    @Test
    public void testRepeatedInsertOverSelectReportsCorrectNumbers() throws Exception {
        Connection conn = methodWatcher.getOrCreateConnection();
        //insert a single record
        conn.createStatement().executeUpdate("insert into T2 (a,b) values (1,1)");
        PreparedStatement ps = conn.prepareStatement("insert into T2 (a,b) select * from T2");
        int iterCount = 10;
        for (int i = 0; i < iterCount; i++) {
            int updateCount = ps.executeUpdate();
            System.out.printf("updateCount=%d%n", updateCount);
//            Assert.assertEquals("Reported incorrect value!",(1<<i),count);
            ResultSet rs = conn.createStatement().executeQuery("select count(*) from T2");
            Assert.assertTrue("Did not return rows for a count query!", rs.next());
            long count = rs.getLong(1);
            System.out.printf("scanCount=%d%n", count);
            Assert.assertEquals("Incorrect inserted records!", (1 << (i + 1)), count);
        }

        ResultSet rs = conn.createStatement().executeQuery("select count(*) from T2");
        Assert.assertTrue("Did not return rows for a count query!", rs.next());
        long count = rs.getLong(1);
        Assert.assertEquals("Incorrect inserted records!", (1 << iterCount), count);
    }

    @Test
    public void testBatchInsert() throws Exception {
        Connection conn = methodWatcher.getOrCreateConnection();
        //insert a single record
        conn.setAutoCommit(false);
        PreparedStatement ps = conn.prepareStatement("insert into batch_test (col1,col2,col3) values (?,?,?)");
        int iterCount = 10;
        for (int i = 0; i < iterCount; i++) {
            ps.setInt(1,i);
            ps.setInt(2,i);
            ps.setInt(3,i);
            ps.addBatch();
        }
        int[] results = ps.executeBatch();
        Assert.assertEquals("results returned correct",10,results.length);
        ps.close();
        ps = conn.prepareStatement("select count(*) from batch_test");
        ResultSet rs = ps.executeQuery();
        rs.next();
        Assert.assertEquals("results returned correct",10,rs.getInt(1));
    }

    @Test
    public void testInsertFromSubselectWithCast() throws Exception {
        Connection conn = methodWatcher.getOrCreateConnection();
        PreparedStatement ps = conn.prepareStatement("insert into t7 values ('Jackson')");
        ps.execute();
        String sql = "insert into T6\n" +
                "SELECT \n" +
                "instr(name, 'ack') as i\n" +
                "FROM \n" +
                "(SELECT \n" +
                "name\t\n" +
                "FROM T7 \n" +
                ") T1";

        // Make sure insert works with a subselect and instr
        ps = conn.prepareStatement(sql);
        int count = ps.executeUpdate();
        Assert.assertTrue(count == 1);

        // verify results
        ps = conn.prepareStatement("select * from t6");
        ResultSet rs = ps.executeQuery();
        Assert.assertTrue(rs.next());
        Assert.assertTrue(rs.getInt(1) == 2);
    }

    @Test
    public void testInsertFromJoinWithImplicitCast() throws Exception {
        Connection conn = methodWatcher.getOrCreateConnection();
        String sql = "insert into TABLE_RESULT select A.CUSTOMER_ID from TABLE_BIGINT A " +
                "where NOT EXISTS (SELECT * from TABLE_DECIMAL B where A.CUSTOMER_ID = B.CUSTOMER_ID)";
        PreparedStatement ps = conn.prepareStatement(sql);
        ps.execute();

        ps = conn.prepareStatement("select * from TABLE_RESULT order by 1");
        ResultSet rs = ps.executeQuery();
        int i = 1;
        while(rs.next()) {
            Assert.assertEquals("Wrong result", i++, rs.getInt(1));
        }
        rs.close();

        ps = conn.prepareStatement("truncate table TABLE_RESULT");
        ps.execute();

        sql = "insert into TABLE_RESULT select A.CUSTOMER_ID from TABLE_BIGINT A " +
                "where NOT EXISTS (SELECT * from TABLE_DECIMAL B where B.CUSTOMER_ID = A.CUSTOMER_ID)";
        ps = conn.prepareStatement(sql);
        ps.execute();

        ps = conn.prepareStatement("select * from TABLE_RESULT order by 1");
        rs = ps.executeQuery();
        i = 1;
        while(rs.next()) {
            Assert.assertEquals("Wrong result", i++, rs.getInt(1));
        }
        rs.close();
    }
}
