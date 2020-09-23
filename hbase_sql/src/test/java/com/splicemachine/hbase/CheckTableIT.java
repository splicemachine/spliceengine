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

package com.splicemachine.hbase;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.derby.impl.storage.TableSplit;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.LongerThanTwoMinutes;
import com.splicemachine.test_tools.TableCreator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.junit.*;
import org.junit.experimental.categories.Category;

import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.*;

/**
 * Created by jyuan on 2/15/18.
 */
@Category(LongerThanTwoMinutes.class)
public class CheckTableIT extends SpliceUnitTest {

    private static final String SCHEMA_NAME = CheckTableIT.class.getSimpleName().toUpperCase();

    private static final String A = "A";
    private static final String AI = "AI";
    private static final String B = "B";
    private static final String BI = "BI";
    private static final String C = "C";
    private static final String D = "D";
    private static final String E = "E";
    private static final String EI = "EI";
    private static final String F = "F";
    private static final String FI = "FI";
    private static final String I = "I";
    private static final String II = "II";
    private static final String H = "H";

    @ClassRule
    public static SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA_NAME);

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA_NAME);

    @BeforeClass
    public static void init() throws Exception {
        Connection conn = spliceClassWatcher.getOrCreateConnection();
        new TableCreator(conn)
                .withCreate("create table A (a int, b int, c int, primary key(a,b))")
                .withIndex("create index AI on A(c)")
                .create();
        spliceClassWatcher.execute("call syscs_util.syscs_split_table_or_index_at_points('CHECKTABLEIT', 'A', null,'\\x83')");
        spliceClassWatcher.execute("call syscs_util.syscs_split_table_or_index_at_points('CHECKTABLEIT', 'A', 'AI','\\x83')");
        spliceClassWatcher.execute("call syscs_util.syscs_perform_major_compaction_on_table('CHECKTABLEIT', 'A')");
        spliceClassWatcher.execute("call syscs_util.syscs_split_table_or_index_at_points('CHECKTABLEIT', 'A', null,'\\x86')");
        spliceClassWatcher.execute("call syscs_util.syscs_split_table_or_index_at_points('CHECKTABLEIT', 'A', 'AI','\\x86')");
        spliceClassWatcher.execute("call syscs_util.syscs_perform_major_compaction_on_table('CHECKTABLEIT', 'A')");
        spliceClassWatcher.execute("insert into A values(1,1,1),(2,2,2),(4,4,4),(5,5,5),(7,7,7)");
        String dir = SpliceUnitTest.getResourceDirectory();
        spliceClassWatcher.execute(String.format("call syscs_util.bulk_import_hfile('CHECKTABLEIT', 'A', null, '%s/check_table.csv','|', null,null,null,null,0,null, true, null, '%s/data', true)", dir, dir));

        new TableCreator(conn)
                .withCreate("create table B (a int, b int, c int, primary key(a,b))")
                .withIndex("create unique index BI on B(c)")
                .create();

        spliceClassWatcher.execute("call syscs_util.syscs_split_table_or_index_at_points('CHECKTABLEIT', 'B', null,'\\x83')");
        spliceClassWatcher.execute("call syscs_util.syscs_split_table_or_index_at_points('CHECKTABLEIT', 'B', 'BI','\\x83')");
        spliceClassWatcher.execute("call syscs_util.syscs_perform_major_compaction_on_table('CHECKTABLEIT', 'B')");
        spliceClassWatcher.execute("call syscs_util.syscs_split_table_or_index_at_points('CHECKTABLEIT', 'B', null,'\\x86')");
        spliceClassWatcher.execute("call syscs_util.syscs_split_table_or_index_at_points('CHECKTABLEIT', 'B', 'BI','\\x86')");
        spliceClassWatcher.execute("call syscs_util.syscs_perform_major_compaction_on_table('CHECKTABLEIT', 'B')");
        spliceClassWatcher.execute("insert into B values (1,1,1),(2,2,2),(4,4,4),(5,5,5),(7,7,7)");
        spliceClassWatcher.execute(String.format("call syscs_util.bulk_import_hfile('CHECKTABLEIT', 'B', null, '%s/check_table.csv','|', null,null,null,null,0,null, true, null, '%s/data', true)", dir, dir));

        int m = 10;
        int n = 10000;
        PreparedStatement ps = spliceClassWatcher.prepareStatement("insert into b values (?,?,?)");
        for (int i = 0; i < m; ++i) {
            for (int j = 10; j < n; ++j) {
                ps.setInt(1,i*n+j);
                ps.setInt(2, i*n+j);
                ps.setInt(3, i*n+j);
                ps.addBatch();
            }
            ps.executeBatch();
        }

        new TableCreator(conn)
                .withCreate("create table C (a int, b int, c int default 10, primary key(a))")
                .withInsert("insert into C(a,b) values(?,?)")
                .withIndex("create index C1 on C(b) exclude null keys")
                .withIndex("create index C2 on C(c) exclude default keys")
                .withIndex("create index C3 on C(a,b) exclude null keys")
                .withRows(rows(
                        row(1, null),
                        row(2, null),
                        row(4, null)))
                .create();
        spliceClassWatcher.execute("insert into C values (5,5,5), (7,7,7)");

        splitTable(SCHEMA_NAME, A, AI);
        splitTable(SCHEMA_NAME, B, BI);

        new TableCreator(conn)
                .withCreate("create table D(a int, b int)")
                .withInsert("insert into D(a,b) values(?,?)")
                .withRows(rows(
                        row(1, 1),
                        row(2, 2),
                        row(3, 3)))
                .create();
        spliceClassWatcher.execute("alter table D add c int not null default 10");
        spliceClassWatcher.execute("create index t1 on D(c, a) exclude null keys");
        spliceClassWatcher.execute("create index t2 on D(c, b) exclude default keys");
        spliceClassWatcher.execute("create index t3 on D(b, c) exclude null keys");

        new TableCreator(conn)
                .withCreate("create table CHECKTABLEIT2.E (a int, b int, c int, primary key(a,b))")
                .withInsert("insert into CHECKTABLEIT2.E values(?,?,?)")
                .withIndex("create index EI on CHECKTABLEIT2.E(c)")
                .withRows(rows(
                        row(1, 1, 1),
                        row(2, 2, 2),
                        row(4, 4, 4),
                        row(5, 5, 5),
                        row(7,7,7)))
                .create();

        spliceClassWatcher.execute("call syscs_util.syscs_split_table_or_index_at_points('CHECKTABLEIT2', 'E', null,'\\x83')");
        spliceClassWatcher.execute("call syscs_util.syscs_split_table_or_index_at_points('CHECKTABLEIT2', 'E', 'EI','\\x83')");
        spliceClassWatcher.execute("call syscs_util.syscs_perform_major_compaction_on_table('CHECKTABLEIT2', 'E')");
        spliceClassWatcher.execute("call syscs_util.syscs_split_table_or_index_at_points('CHECKTABLEIT2', 'E', null,'\\x86')");
        spliceClassWatcher.execute("call syscs_util.syscs_split_table_or_index_at_points('CHECKTABLEIT2', 'E', 'EI','\\x86')");
        deleteFirstIndexRegion(spliceClassWatcher, conn, "CHECKTABLEIT2", E, EI);


        new TableCreator(conn)
                .withCreate("create table CHECKTABLEIT2.F (a int, b int, c int, primary key(a,b))")
                .withInsert("insert into CHECKTABLEIT2.F values(?,?,?)")
                .withIndex("create unique index CHECKTABLEIT2.FI on CHECKTABLEIT2.F(c)")
                .withRows(rows(
                        row(0, 0, 0)))
                .create();

        spliceClassWatcher.execute("call syscs_util.syscs_split_table_or_index_at_points('CHECKTABLEIT2', 'F', null,'\\x83')");
        spliceClassWatcher.execute("call syscs_util.syscs_split_table_or_index_at_points('CHECKTABLEIT2', 'F', 'FI','\\x83')");
        spliceClassWatcher.execute("call syscs_util.syscs_perform_major_compaction_on_table('CHECKTABLEIT2', 'F')");
        spliceClassWatcher.execute("call syscs_util.syscs_split_table_or_index_at_points('CHECKTABLEIT2', 'F', null,'\\x86')");
        spliceClassWatcher.execute("call syscs_util.syscs_split_table_or_index_at_points('CHECKTABLEIT2', 'F', 'FI','\\x86')");

        ps = spliceClassWatcher.prepareStatement("insert into CHECKTABLEIT2.F select * from B");
        ps.execute();
        deleteFirstIndexRegion(spliceClassWatcher, conn, "CHECKTABLEIT2", F, FI);

        new TableCreator(conn)
                .withCreate("create table G(i int)")
                .withIndex("create index GI on G(I)")
                .create();

        new TableCreator(conn)
                .withCreate("create table H(i int, j int, primary key(i))")
                .withInsert("insert into H(i, j) values(?,?)")
                .withRows(rows(
                        row(1, 1),
                        row(2, 2),
                        row(3, 3)))
                .create();

        new TableCreator(conn)
                .withCreate("create table I(i int, j int, primary key(i))")
                .withInsert("insert into I(i, j) values(?,?)")
                .withIndex("create unique index II on I(j)")
                .withRows(rows(
                        row(1, 1),
                        row(2, 2),
                        row(3, 3)))
                .create();

        new TableCreator(conn)
                    .withCreate("create table T9863(i int)")
                    .withIndex("create unique index I9863 on T9863(I)")
                    .create();
    }

    @AfterClass
    public static void dropTables() throws Exception {
        spliceClassWatcher.execute("drop table CHECKTABLEIT2.E");
        spliceClassWatcher.execute("drop table CHECKTABLEIT2.F");
    }

    @Test
    public void testSystemTable() throws Exception {
        // delete one row from SYSCONGLOMERATES_INDEX2
        ResultSet rs = spliceClassWatcher.executeQuery("select rowid from sys.sysconglomerates --splice-properties index=SYSCONGLOMERATES_INDEX2\n" +
                "where conglomeratename='GI'");
        rs.next();
        String rowid = rs.getString(1);
        rs = spliceClassWatcher.executeQuery("select conglomeratenumber from sys.sysconglomerates --splice-properties index=SYSCONGLOMERATES_INDEX2\n" +
                "where conglomeratename='SYSCONGLOMERATES_INDEX2'");
        rs.next();
        long index2 = rs.getLong(1);
        rs.close();
        spliceClassWatcher.execute(String.format("call syscs_util.syscs_dictionary_delete(%d, '%s')",
               index2, rowid));

        // delete one row from SYSCONGLOMERATES_INDEX1
        rs = spliceClassWatcher.executeQuery("select conglomerateid from sys.sysconglomerates --splice-properties index=SYSCONGLOMERATES_INDEX1\n" +
                "where conglomeratename='GI'");
        rs.next();
        String conglomerateId = rs.getString(1);
        rs.close();

        rs = spliceClassWatcher.executeQuery(String.format("select rowid from sys.sysconglomerates --splice-properties index=SYSCONGLOMERATES_INDEX1\n" +
                "where conglomerateid='%s'", conglomerateId));
        rs.next();
        rowid = rs.getString(1);
        rs.close();

        rs = spliceClassWatcher.executeQuery("select conglomeratenumber from sys.sysconglomerates --splice-properties index=null\n" +
                "where conglomeratename='SYSCONGLOMERATES_INDEX1'");
        rs.next();
        long index1 = rs.getLong(1);
        rs.close();

        spliceClassWatcher.execute(String.format("call syscs_util.syscs_dictionary_delete(%d, '%s')",
                index1, rowid));

        // Repair missing indexes
        spliceClassWatcher.execute(String.format("call syscs_util.fix_table('SYS', 'SYSCONGLOMERATES', null, '%s/fix-conglomerates.out')", getResourceDirectory()));
        String select =
                "SELECT \"message\" " +
                        "from new com.splicemachine.derby.vti.SpliceFileVTI(" +
                        "'%s',NULL,'|',NULL,'HH:mm:ss','yyyy-MM-dd','yyyy-MM-dd HH:mm:ss','true','UTF-8' ) " +
                        "AS messages (\"message\" varchar(200)) order by 1";
        rs = spliceClassWatcher.executeQuery(format(select, String.format("%s/fix-conglomerates.out", getResourceDirectory())));
        String s = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        rs.close();

        rs = spliceClassWatcher.executeQuery("select rowid from sys.sysconglomerates --splice-properties index=null\n" +
                "where conglomeratename='GI'");
        rs.next();
        rowid = rs.getString(1);

        // depending on the rowid, the order of the strings below could be different, so need a sort
        String expected = String.format("message                                    |\n" +
                "--------------------------------------------------------------------------------\n" +
                "                               %s                                |\n" +
                "                               %s                                |\n" +
                "Created indexes for the following 1 rows from base table SYS.SYSCONGLOMERATES: |\n" +
                "Created indexes for the following 1 rows from base table SYS.SYSCONGLOMERATES: |\n" +
                "                           SYSCONGLOMERATES_INDEX1:                            |\n" +
                "                           SYSCONGLOMERATES_INDEX2:                            |", rowid, rowid);


        String sortedExpected = Stream.of(expected.split("\n")).sorted().collect(Collectors.joining("\n"));
        String sortedS = Stream.of(s.split("\n")).sorted().collect(Collectors.joining("\n"));

        assertEquals(sortedS, sortedExpected, sortedS);

        // Check the table again
        rs = spliceClassWatcher.executeQuery(String.format("call syscs_util.check_table('SYS', 'SYSCONGLOMERATES', null, 2, '%s/fix-conglomerates.out')", getResourceDirectory()));
        rs.next();
        s = rs.getString(1);
        assertEquals(s, s, "No inconsistencies were found.");
    }

    @Test
    public void testCheckTable() throws Exception {

        checkTable(SCHEMA_NAME, A, AI, Arrays.asList(
                "                 { 1, 810081 }@8100C0C09090=>810081                  |\n",
                "                 { 2, 820082 }@8200C18090A0=>820082                  |\n",
                "                 { 7, 870087 }@8700C3C090F0=>870087                  |\n"));

        checkTable(SCHEMA_NAME, B, BI, Arrays.asList(
                "                      { 1, 810081 }@81=>810081                       |\n",
                "                      { 2, 820082 }@82=>820082                       |\n",
                "                      { 7, 870087 }@87=>870087                       |\n"));
        testChekSchema();
        removeDuplicateIndexes();
    }

    public void removeDuplicateIndexes() throws Exception {
        spliceClassWatcher.execute(String.format("call syscs_util.fix_table('%s', '%s', null, '%s/fix-%s.out')", SCHEMA_NAME, A, getResourceDirectory(), A));
        String select =
                "SELECT \"message\" " +
                        "from new com.splicemachine.derby.vti.SpliceFileVTI(" +
                        "'%s',NULL,'|',NULL,'HH:mm:ss','yyyy-MM-dd','yyyy-MM-dd HH:mm:ss','true','UTF-8' ) " +
                        "AS messages (\"message\" varchar(200)) order by 1";
        ResultSet rs =spliceClassWatcher.executeQuery(format(select, String.format("%s/fix-%s.out", getResourceDirectory(), A)));
        String s = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);

        String expected = "message                                 |\n" +
                "--------------------------------------------------------------------------\n" +
                "Created indexes for the following 2 rows from base table CHECKTABLEIT.A: |\n" +
                "                    Removed the following 1 indexes:                     |\n" +
                "                  The following 2 indexes are deleted:                   |\n" +
                "                   { 1, 810081 }@8100C0C09090=>810081                    |\n" +
                "                   { 2, 820082 }@8200C18090A0=>820082                    |\n" +
                "                                { 4, 4 }                                 |\n" +
                "                                { 5, 5 }                                 |\n" +
                "                   { 7, 870087 }@8700C3C090F0=>870087                    |\n" +
                "                                   AI:                                   |";
        assertEquals(s, expected, s);
        rs = spliceClassWatcher.executeQuery(String.format("call syscs_util.check_table('%s', '%s', null, 2, '%s/check-%s2.out')", SCHEMA_NAME, A, getResourceDirectory(), A));
        rs.next();
        s = rs.getString(1);
        assertEquals(s, "No inconsistencies were found.", s);
    }

    @Test
    public void testFixTableOnSpark() throws Exception {
        spliceClassWatcher.execute(String.format("call syscs_util.check_table('%s', '%s', null, 1, '%s/check-%s2.out')", "CHECKTABLEIT2", F, getResourceDirectory(), F));
        String select =
                "SELECT \"message\" " +
                        "from new com.splicemachine.derby.vti.SpliceFileVTI(" +
                        "'%s',NULL,'|',NULL,'HH:mm:ss','yyyy-MM-dd','yyyy-MM-dd HH:mm:ss','true','UTF-8' ) " +
                        "AS messages (\"message\" varchar(200)) order by 1";
        ResultSet rs =spliceClassWatcher.executeQuery(format(select, String.format("%s/check-%s2.out", getResourceDirectory(), F)));
        String s = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        String expected ="message    |\n" +
                "---------------\n" +
                "count = 99903 |\n" +
                "count = 99904 |\n" +
                "     F:       |\n" +
                "     FI:      |";
        assertEquals(s, expected, s);

        spliceClassWatcher.execute(String.format("call syscs_util.fix_table('%s', '%s', null, '%s/check-%s2.out')", "CHECKTABLEIT2", F, getResourceDirectory(), F));
        rs = spliceClassWatcher.executeQuery(String.format("call syscs_util.check_table('%s', '%s', null, 1, '%s/check-%s2.out')", "CHECKTABLEIT2", F, getResourceDirectory(), F));
        rs.next();
        s = rs.getString(1);
        assertEquals(s, s, "No inconsistencies were found.");
    }

    @Test
    public void testFixTable() throws Exception {
        spliceClassWatcher.execute(String.format("call syscs_util.check_table('%s', '%s', null, 2, '%s/check-%s2.out')", "CHECKTABLEIT2", E, getResourceDirectory(), E));
        String select =
                "SELECT \"message\" " +
                        "from new com.splicemachine.derby.vti.SpliceFileVTI(" +
                        "'%s',NULL,'|',NULL,'HH:mm:ss','yyyy-MM-dd','yyyy-MM-dd HH:mm:ss','true','UTF-8' ) " +
                        "AS messages (\"message\" varchar(200)) order by 1";
        ResultSet rs =spliceClassWatcher.executeQuery(format(select, String.format("%s/check-%s2.out", getResourceDirectory(), E)));
        String s = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        String expected = "message                                |\n" +
                "-----------------------------------------------------------------------\n" +
                "The following 2 rows from base table CHECKTABLEIT2.E are not indexed: |\n" +
                "                              { 1, 1 }                                |\n" +
                "                              { 2, 2 }                                |\n" +
                "                                 EI:                                  |";
        assertEquals(s, expected, s);

        spliceClassWatcher.execute(String.format("call syscs_util.fix_table('%s', '%s', null, '%s/fix-%s.out')", "CHECKTABLEIT2", E, getResourceDirectory(), E));
        select =
                "SELECT \"message\" " +
                        "from new com.splicemachine.derby.vti.SpliceFileVTI(" +
                        "'%s',NULL,'|',NULL,'HH:mm:ss','yyyy-MM-dd','yyyy-MM-dd HH:mm:ss','true','UTF-8' ) " +
                        "AS messages (\"message\" varchar(200)) order by 1";
        rs =spliceClassWatcher.executeQuery(format(select, String.format("%s/fix-%s.out", getResourceDirectory(), E)));
        s = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        expected = "message                                  |\n" +
                "---------------------------------------------------------------------------\n" +
                "Created indexes for the following 2 rows from base table CHECKTABLEIT2.E: |\n" +
                "                                { 1, 1 }                                  |\n" +
                "                                { 2, 2 }                                  |\n" +
                "                                   EI:                                    |";
        assertEquals(s, expected, s);
        rs = spliceClassWatcher.executeQuery(String.format("call syscs_util.check_table('%s', '%s', null, 2, '%s/check-%s2.out')", "CHECKTABLEIT2", E, getResourceDirectory(), E));
        rs.next();
        s = rs.getString(1);
        assertEquals(s, s, "No inconsistencies were found.");
    }
    @Test
    public void checkTableWithSkippedNullValues() throws Exception {
        ResultSet rs = spliceClassWatcher.executeQuery(String.format("call syscs_util.check_table('%s', '%s', null, 2, '%s/check-%s2.out')", SCHEMA_NAME, D, getResourceDirectory(), D));
        rs.next();
        String s = rs.getString(1);
        assertEquals(s, s, "No inconsistencies were found.");
    }

    public void testChekSchema() throws Exception {

        spliceClassWatcher.execute(String.format("call syscs_util.check_table('%s', null, null, 1, '%s/check-%s1.out')", SCHEMA_NAME, getResourceDirectory(), SCHEMA_NAME));
        String select =
                "SELECT \"message\" " +
                        "from new com.splicemachine.derby.vti.SpliceFileVTI(" +
                        "'%s',NULL,'|',NULL,'HH:mm:ss','yyyy-MM-dd','yyyy-MM-dd HH:mm:ss','true','UTF-8' ) " +
                        "AS messages (\"message\" varchar(200)) order by 1";
        ResultSet rs =spliceClassWatcher.executeQuery(format(select, String.format("%s/check-%s1.out", getResourceDirectory(), SCHEMA_NAME)));
        String s = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        String expected =
                "message                                                                    |\n" +
                "------------------------------------------------------------------------------------------------------------------------------------------------\n" +
                "Index count and base table count are not expected to match because index excludes null or default keys. The index should be checked at level 2 |\n" +
                "Index count and base table count are not expected to match because index excludes null or default keys. The index should be checked at level 2 |\n" +
                "Index count and base table count are not expected to match because index excludes null or default keys. The index should be checked at level 2 |\n" +
                "Index count and base table count are not expected to match because index excludes null or default keys. The index should be checked at level 2 |\n" +
                "Index count and base table count are not expected to match because index excludes null or default keys. The index should be checked at level 2 |\n" +
                "Index count and base table count are not expected to match because index excludes null or default keys. The index should be checked at level 2 |\n" +
                "                                                                   count = 3                                                                   |\n" +
                "                                                                   count = 3                                                                   |\n" +
                "                                                                   count = 4                                                                   |\n" +
                "                                                                   count = 5                                                                   |\n" +
                "                                                                 count = 99903                                                                 |\n" +
                "                                                                 count = 99904                                                                 |\n" +
                "                                                                      A:                                                                       |\n" +
                "                                                                      AI:                                                                      |\n" +
                "                                                                      B:                                                                       |\n" +
                "                                                                      BI:                                                                      |\n" +
                "                                                                      C1:                                                                      |\n" +
                "                                                                      C2:                                                                      |\n" +
                "                                                                      C3:                                                                      |\n" +
                "                                                                      C:                                                                       |\n" +
                "                                                                      D:                                                                       |\n" +
                "                                                                      T1:                                                                      |\n" +
                "                                                                      T2:                                                                      |\n" +
                "                                                                      T3:                                                                      |";

        assertEquals(s, expected, s);

        spliceClassWatcher.execute(String.format("call syscs_util.check_table('%s', null, null, 2, '%s/check-%s2.out')", SCHEMA_NAME, getResourceDirectory(), SCHEMA_NAME));
        select =
                "SELECT \"message\" " +
                        "from new com.splicemachine.derby.vti.SpliceFileVTI(" +
                        "'%s',NULL,'|',NULL,'HH:mm:ss','yyyy-MM-dd','yyyy-MM-dd HH:mm:ss','true','UTF-8' ) " +
                        "AS messages (\"message\" varchar(200)) order by 1";
        rs =spliceClassWatcher.executeQuery(format(select, String.format("%s/check-%s2.out", getResourceDirectory(), SCHEMA_NAME)));
        s = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        expected = "message                               |\n" +
                "----------------------------------------------------------------------\n" +
                "               The following 1 indexes are duplicates:               |\n" +
                "               The following 1 indexes are duplicates:               |\n" +
                "                The following 2 indexes are invalid:                 |\n" +
                "                The following 2 indexes are invalid:                 |\n" +
                "The following 2 rows from base table CHECKTABLEIT.A are not indexed: |\n" +
                "The following 2 rows from base table CHECKTABLEIT.B are not indexed: |\n" +
                "                 { 1, 810081 }@8100C0C09090=>810081                  |\n" +
                "                      { 1, 810081 }@81=>810081                       |\n" +
                "                 { 2, 820082 }@8200C18090A0=>820082                  |\n" +
                "                      { 2, 820082 }@82=>820082                       |\n" +
                "                              { 4, 4 }                               |\n" +
                "                              { 4, 4 }                               |\n" +
                "                              { 5, 5 }                               |\n" +
                "                              { 5, 5 }                               |\n" +
                "                 { 7, 870087 }@8700C3C090F0=>870087                  |\n" +
                "                      { 7, 870087 }@87=>870087                       |\n" +
                "                                 AI:                                 |\n" +
                "                                 BI:                                 |";

        assertEquals(s, expected, s);
    }

    @Test
    public void testIndexExcludeDefaults() throws Exception {

        spliceClassWatcher.execute(String.format("call syscs_util.check_table('%s', '%s', null, 1, '%s/check-%s1.out')", SCHEMA_NAME, C, getResourceDirectory(), C));
        String select =
                "SELECT \"message\" " +
                        "from new com.splicemachine.derby.vti.SpliceFileVTI(" +
                        "'%s',NULL,'|',NULL,'HH:mm:ss','yyyy-MM-dd','yyyy-MM-dd HH:mm:ss','true','UTF-8' ) " +
                        "AS messages (\"message\" varchar(200)) order by 1";
        ResultSet rs =spliceClassWatcher.executeQuery(format(select, String.format("%s/check-%s1.out", getResourceDirectory(), C)));
        String s = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        String expected =
                "message                                                                    |\n" +
                "------------------------------------------------------------------------------------------------------------------------------------------------\n" +
                "Index count and base table count are not expected to match because index excludes null or default keys. The index should be checked at level 2 |\n" +
                "Index count and base table count are not expected to match because index excludes null or default keys. The index should be checked at level 2 |\n" +
                "Index count and base table count are not expected to match because index excludes null or default keys. The index should be checked at level 2 |\n" +
                "                                                                   count = 5                                                                   |\n" +
                "                                                                      C1:                                                                      |\n" +
                "                                                                      C2:                                                                      |\n" +
                "                                                                      C3:                                                                      |\n" +
                "                                                                      C:                                                                       |";
        assertEquals(s, s, expected);

        rs = spliceClassWatcher.executeQuery(String.format("call syscs_util.check_table('%s', '%s', null, 2, '%s/check-%s2.out')", SCHEMA_NAME, C, getResourceDirectory(), C));
        rs.next();
        s = rs.getString(1);
        assertEquals(s, s, "No inconsistencies were found.");
    }

    @Test
    public void testDB_9863() throws Exception {
        try (Connection c = spliceClassWatcher.createConnection();
            Statement st = c.createStatement()) {
            c.setAutoCommit(false);

            st.executeUpdate("insert into T9863 values 1");
            st.executeUpdate("delete from T9863");
            st.executeUpdate("insert into T9863 values 1");

            c.rollback();

            st.executeUpdate("insert into T9863 values 1");
            c.commit();

            try (ResultSet rs = st.executeQuery(String.format("call syscs_util.check_table('%s', 'T9863', null, 2, '%s/check-T9863.out')", SCHEMA_NAME, getResourceDirectory()))) {
                assertTrue(rs.next());
                assertEquals("No inconsistencies were found.", rs.getString(1));
            }
        }
    }

    @Test
    public void testForeignKeyUniqueIndex() throws Exception {
        spliceClassWatcher.execute("alter table I add CONSTRAINT fc FOREIGN KEY(j) REFERENCES H(i) ON UPDATE NO ACTION ON DELETE restrict");
        ResultSet rs = spliceClassWatcher.executeQuery(String.format("call syscs_util.check_table('%s', '%s', null, 2, '%s/check-%s2.out')", SCHEMA_NAME, I, getResourceDirectory(), I));
        rs.next();
        String s = rs.getString(1);
        Assert.assertEquals(s, s, "No inconsistencies were found.");
    }
    @Test
    public void negativeTests() throws Exception {


        try {
            spliceClassWatcher.execute(String.format("call syscs_util.check_table('%s', '%s', null, 1, null)", SCHEMA_NAME, A, getResourceDirectory(), A));
            fail("Should fail!");
        }
        catch (SQLException e) {
            assertEquals(e.getSQLState(), "TS008");
        }

        try {
            spliceClassWatcher.execute(String.format("call syscs_util.check_table('%s', '%s', null, 1, ' ')", SCHEMA_NAME, A, getResourceDirectory(), A));
            fail("Should fail!");
        }

        catch (SQLException e) {
            assertEquals(e.getSQLState(), "TS008");
        }

        try {
            spliceClassWatcher.execute(String.format("call syscs_util.check_table('%s', '%s', null, 3, '%s/check-%s.out')", SCHEMA_NAME, A, getResourceDirectory(), A));
            fail("Should fail!");
        }
        catch (SQLException e) {
            assertEquals(e.getSQLState(), "TS007");
        }

        try {
            spliceClassWatcher.execute(String.format("call syscs_util.check_table('%s', '%s', 'XY', 2, '%s/check-%s.out')", SCHEMA_NAME, A, getResourceDirectory(), A));
            fail("Should fail!");
        }
        catch (SQLException e) {
            assertEquals(e.getSQLState(), "42X65");
        }

        try {
            spliceClassWatcher.execute(String.format("call syscs_util.check_table('%s', null, 'AI', 2, '%s/check-%s.out')", SCHEMA_NAME, getResourceDirectory(), A));
            fail("Should fail!");
        }
        catch (SQLException e) {
            assertEquals(e.getSQLState(), "TS008");
        }
    }

    private void checkTable(String schemaName, String tableName, String indexName, List<String> expectedRowIds) throws Exception {

        //Run check_table
        spliceClassWatcher.execute(String.format("call syscs_util.check_table('%s', '%s', null, 2, '%s/check-%s.out')", schemaName, tableName, getResourceDirectory(), tableName));
        String select =
                "SELECT \"message\" " +
                        "from new com.splicemachine.derby.vti.SpliceFileVTI(" +
                        "'%s',NULL,'|',NULL,'HH:mm:ss','yyyy-MM-dd','yyyy-MM-dd HH:mm:ss','true','UTF-8' ) " +
                        "AS messages (\"message\" varchar(200)) order by 1";
        ResultSet rs =spliceClassWatcher.executeQuery(format(select, String.format("%s/check-%s.out", getResourceDirectory(), tableName)));
        String s = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        String expected = format("message                               |\n" +
                "----------------------------------------------------------------------\n" +
                "               The following 1 indexes are duplicates:               |\n" +
                "                The following 2 indexes are invalid:                 |\n" +
                "The following 2 rows from base table CHECKTABLEIT.%s are not indexed: |\n" +
                expectedRowIds.get(0) +
                expectedRowIds.get(1) +
                "                              { 4, 4 }                               |\n" +
                "                              { 5, 5 }                               |\n" +
                expectedRowIds.get(2) +
                "                                 %s:                                 |", tableName, indexName);

        assertEquals(s, s, expected);
    }

    private static void splitTable(String schemaName, String tableName, String indexName) throws Exception {
        Connection connection = spliceClassWatcher.getOrCreateConnection();
        SConfiguration config = HConfiguration.getConfiguration();
        HBaseTestingUtility testingUtility = new HBaseTestingUtility((Configuration) config.getConfigSource().unwrapDelegate());
        HBaseAdmin admin = testingUtility.getHBaseAdmin();

        // Delete 1st region of the table
        long conglomerateId = TableSplit.getConglomerateId(connection, schemaName, tableName, null);
        TableName tName = TableName.valueOf(config.getNamespace(),Long.toString(conglomerateId));
        List<HRegionInfo> partitions = admin.getTableRegions(tName);
        for (HRegionInfo partition : partitions) {
            byte[] startKey = partition.getStartKey();
            if (startKey.length == 0) {
                String encodedRegionName = partition.getEncodedName();
                spliceClassWatcher.execute(String.format("call syscs_util.delete_region('%s', '%s', null, '%s', false)",
                        schemaName, tableName, encodedRegionName));
                break;
            }
        }

        // Delete 2nd region of index
        conglomerateId = TableSplit.getConglomerateId(connection, schemaName, tableName, indexName);
        TableName iName = TableName.valueOf(config.getNamespace(),Long.toString(conglomerateId));
        partitions = admin.getTableRegions(iName);
        for (HRegionInfo partition : partitions) {
            byte[] startKey = partition.getStartKey();
            byte[] endKey = partition.getEndKey();
            if (startKey.length != 0 && endKey.length != 0) {
                String encodedRegionName = partition.getEncodedName();
                spliceClassWatcher.execute(String.format("call syscs_util.delete_region('%s', '%s', '%s', '%s', false)",
                        schemaName, tableName, indexName, encodedRegionName));
                break;
            }
        }
    }

    public static void deleteFirstIndexRegion(SpliceWatcher spliceWatcher, Connection connection, String schemaName, String tableName, String indexName) throws Exception {
        SConfiguration config = HConfiguration.getConfiguration();
        HBaseTestingUtility testingUtility = new HBaseTestingUtility((Configuration) config.getConfigSource().unwrapDelegate());
        Admin admin = testingUtility.getAdmin();

        // Delete 2nd region of index
        long   conglomerateId = TableSplit.getConglomerateId(connection, schemaName, tableName, indexName);
        TableName iName = TableName.valueOf(config.getNamespace(),Long.toString(conglomerateId));
        List<RegionInfo> partitions = admin.getRegions(iName);
        for (RegionInfo partition : partitions) {
            byte[] startKey = partition.getStartKey();
            if (startKey.length == 0) {
                String encodedRegionName = partition.getEncodedName();
                spliceWatcher.execute(String.format("call syscs_util.delete_region('%s', '%s', '%s', '%s', false)",
                        schemaName, tableName, indexName, encodedRegionName));
                break;
            }
        }
    }
}
