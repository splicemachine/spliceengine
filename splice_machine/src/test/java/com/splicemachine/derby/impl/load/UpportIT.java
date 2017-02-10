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

package com.splicemachine.derby.impl.load;

import org.spark_project.guava.collect.Lists;
import org.spark_project.guava.primitives.Ints;
import com.splicemachine.derby.test.framework.SpliceNetConnection;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.TestConnection;

import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.File;
import java.sql.*;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import static org.junit.Assert.assertNotNull;

/**
 * Correctness tests around the "UPSERT_FROM_FILE" or "UPPORT" functionality.
 *
 * @author Scott Fines
 *         Date: 10/20/14
 */
public class UpportIT extends SpliceUnitTest {
    private static final SpliceSchemaWatcher schema =
            new SpliceSchemaWatcher(UpportIT.class.getSimpleName().toUpperCase());
    private static final SpliceTableWatcher nullableBTable =
            new SpliceTableWatcher("empty_table",schema.schemaName,"(a int, b int, primary key(a))");
    private static final SpliceTableWatcher occupiedTable =
            new SpliceTableWatcher("occ_table",schema.schemaName,"(a int, b int not null, primary key(a))");
    private static final SpliceTableWatcher no_pk  =
            new SpliceTableWatcher("no_pk",schema.schemaName,"(a int, b int)");

    private static TestConnection conn;
    private static File BADDIR;

    @ClassRule public static TestRule chain = RuleChain.outerRule(schema)
            .around(nullableBTable)
            .around(occupiedTable)
            .around(no_pk);
    private static int size;
    private static SpliceUnitTest.TestFileGenerator fullTestFile;
    private static List<int[]> correctFullData;
    private static SpliceUnitTest.TestFileGenerator partialTestFile;
    private static List<int[]> correctPartialData;
    private static SpliceUnitTest.TestFileGenerator fullTestFileWithDuplicates;

    @BeforeClass
    public static void setUpClass() throws Exception {

        BADDIR = createBadLogDirectory(schema.schemaName);
        assertNotNull(BADDIR);
        File IMPORTDIR = createImportFileDirectory(schema.schemaName);
        assertNotNull(IMPORTDIR);

        size = 5;
        correctFullData = Lists.newArrayListWithExpectedSize(size);
        fullTestFile = generateFullRow(IMPORTDIR, "full", size, correctFullData, false);

        List<int[]> correctFullDataWithDuplicates = Lists.newArrayListWithExpectedSize(size); //thrown away
        fullTestFileWithDuplicates = generateFullRow(IMPORTDIR, "fullWithDuplicates", size, correctFullDataWithDuplicates, true);

        correctPartialData = Lists.newArrayListWithExpectedSize(size);
        partialTestFile = generatePartialRow(IMPORTDIR, "partial", size, correctPartialData);
    }

    @Before
    public void before() throws Exception {
        conn = createConnection();
        conn.setAutoCommit(false);
    }

    @After
    public void tearDown() throws Exception {
        try {
            conn.rollback();
        } catch (SQLException e) {
            // ignore
        }
        try {
            conn.close();
        } catch (SQLException e) {
            // ignore
        }
    }

    private static final Comparator<int[]> intArrayComparator = new Comparator<int[]>() {
        @Override public int compare(int[] o1, int[] o2) { return Ints.compare(o1[0], o2[0]); }
    };

    @Test
    public void testUpsertFailsWithMissingNonNullColumn() throws Exception {
        CallableStatement statement =
                conn.prepareCall("call SYSCS_UTIL.UPSERT_DATA_FROM_FILE(?,?,?,?,null,null,null,null,null,0,?,null,null)");
        statement.setString(1,schema.schemaName);
        statement.setString(2,occupiedTable.tableName);
        statement.setString(3,"a");
        statement.setString(4,fullTestFile.getFilePath());
        statement.setString(5,BADDIR.getCanonicalPath());

        try{
            statement.execute();
            Assert.fail("Did not throw exception");
        }catch(SQLException se){
            SpliceUnitTest.assertBadFileContainsError(BADDIR, fullTestFile.getFileName(), "23502", null);
        }
    }

    @Test
    public void testUpsertFailsWithMissingPk() throws Exception {
        CallableStatement statement =
                conn.prepareCall("call SYSCS_UTIL.UPSERT_DATA_FROM_FILE(?,?,?,?,null,null,null,null,null,0,?,null,null)");
        statement.setString(1,schema.schemaName);
        statement.setString(2, nullableBTable.tableName);
        statement.setString(3,"b");
        statement.setString(4,fullTestFile.getFilePath());
        statement.setString(5,BADDIR.getCanonicalPath());

        try{
            statement.execute();
            Assert.fail("Did not throw exception");
        }catch(SQLException se){
            SpliceUnitTest.assertBadFileContainsError(BADDIR, fullTestFile.getFileName(), "23502", null);
        }
    }

    @Test
    public void testUpsertFailsWithNoPk() throws Exception {
        CallableStatement statement =
                conn.prepareCall("call SYSCS_UTIL.UPSERT_DATA_FROM_FILE(?,?,null,?,null,null,null,null,null,0,?,null,null)");
        statement.setString(1,schema.schemaName);
        statement.setString(2,no_pk.tableName);
        statement.setString(3,fullTestFile.getFilePath());
        statement.setString(4,BADDIR.getCanonicalPath());

        try{
            statement.execute();
            Assert.fail("Did not throw exception");
        }catch(SQLException se){
            Assert.assertEquals("Incorrect SQL State!", "SE018", se.getSQLState());
        }
    }

    @Test
    public void testUpsertIntoOccupiedTableWillUpdate() throws Exception {
        PreparedStatement ps = conn.prepareStatement("insert into " + occupiedTable + "(a,b) values (?,?)");
        List<int[]> newCorrect = Lists.newArrayList(correctFullData);
        for(int[] correctRow:newCorrect){
            //add a row that's different
            ps.setInt(1,correctRow[0]); ps.setInt(2,correctRow[1]/2); ps.executeUpdate();
        }
        //add one extra row so that we know that we don't overwrite the entire table
        ps.setInt(1,size+1); ps.setInt(2,size+1); ps.executeUpdate();
        newCorrect.add(new int[]{size + 1, size + 1});

        CallableStatement statement =
                conn.prepareCall("call SYSCS_UTIL.UPSERT_DATA_FROM_FILE(?,?,null,?,null,null,null,null,null,0,?,null,null)");
        statement.setString(1,schema.schemaName);
        statement.setString(2,occupiedTable.tableName);
        statement.setString(3,fullTestFile.getFilePath());
        statement.setString(4,BADDIR.getCanonicalPath());

        ResultSet resultSet = statement.executeQuery();
        //make sure that the bad records list is good
        validateImportResults(resultSet,size,0);

        //make sure that the data matches
        List<int[]> actualData = Lists.newArrayListWithExpectedSize(size);
        ResultSet rs = conn.query("select * from "+ occupiedTable);
        while(rs.next()){
            int[] data = new int[2];
            data[0] = rs.getInt(1);
            Assert.assertFalse("Column a was null!",rs.wasNull());
            data[1] = rs.getInt(2);
            Assert.assertFalse("Column b was null!",rs.wasNull());
            actualData.add(data);
        }

        Collections.sort(newCorrect, intArrayComparator);
        Collections.sort(actualData, intArrayComparator);
        assertCorrectResult(newCorrect, actualData);
    }

    @Test
    public void testUpsertIntoOccupiedPartiallyTableWillUpdate() throws Exception {
        PreparedStatement ps = conn.prepareStatement("insert into " + nullableBTable + "(a,b) values (?,?)");
        List<int[]> newCorrect = Lists.newArrayListWithExpectedSize(size);
        for(int[] correctRow:correctFullData){
            //add a row that's different
            ps.setInt(1,correctRow[0]); ps.setInt(2,correctRow[1]/2); ps.executeUpdate();
            newCorrect.add(new int[]{correctRow[0],correctRow[1]/2});
        }
        //add one extra row so that we know that we don't overwrite the entire table
        ps.setInt(1,size+1); ps.setInt(2,size+1); ps.executeUpdate();
        newCorrect.add(new int[]{size + 1, size + 1});

        CallableStatement statement =
                conn.prepareCall("call SYSCS_UTIL.UPSERT_DATA_FROM_FILE(?,?,?,?,null,null,null,null,null,0,?,null,null)");
        statement.setString(1,schema.schemaName);
        statement.setString(2,nullableBTable.tableName);
        statement.setString(3,"a");
        statement.setString(4,partialTestFile.getFilePath());
        statement.setString(5,BADDIR.getCanonicalPath());

        ResultSet resultSet = statement.executeQuery();
        //make sure that the bad records list is good
        validateImportResults(resultSet,size,0);

        //make sure that the data matches
        List<int[]> actualData = Lists.newArrayListWithExpectedSize(size);
        ResultSet rs = conn.query("select * from "+ nullableBTable);
        while(rs.next()){
            int[] data = new int[2];
            data[0] = rs.getInt(1);
            Assert.assertFalse("Column a was null!",rs.wasNull());
            data[1] = rs.getInt(2);
            Assert.assertFalse("Column b was null!",rs.wasNull());
            actualData.add(data);
        }

        Collections.sort(newCorrect, intArrayComparator);
        Collections.sort(actualData, intArrayComparator);
        assertCorrectResult(newCorrect, actualData);
    }

    @Test
    public void testUpsertWithPartialEmptyTableWillInsert() throws Exception {
        CallableStatement statement =
                conn.prepareCall("call SYSCS_UTIL.UPSERT_DATA_FROM_FILE(?,?,?,?,null,null,null,null,null,0,?,null,null)");
        statement.setString(1,schema.schemaName);
        statement.setString(2,nullableBTable.tableName);
        statement.setString(3,"a");
        statement.setString(4,partialTestFile.getFilePath());
        statement.setString(5,BADDIR.getCanonicalPath());

        ResultSet resultSet = statement.executeQuery();
        //make sure that the bad records list is good
        validateImportResults(resultSet,size,0);

        //make sure that the data matches
        List<int[]> actualData = Lists.newArrayListWithExpectedSize(size);
        ResultSet rs = conn.query("select * from "+ nullableBTable);
        while(rs.next()){
            int[] data = new int[2];
            data[0] = rs.getInt(1);
            Assert.assertFalse("Column a was null!",rs.wasNull());
            data[1] = rs.getInt(2);
            Assert.assertTrue("Column b was null!", rs.wasNull());
            actualData.add(data);
        }

        Collections.sort(correctPartialData, intArrayComparator);
        Collections.sort(actualData, intArrayComparator);
        assertCorrectResult(correctPartialData,actualData);
    }

    @Test
    public void testUpsertWithEmptyTableWillInsert() throws Exception {
        CallableStatement statement =
                conn.prepareCall("call SYSCS_UTIL.UPSERT_DATA_FROM_FILE(?,?,null,?,null,null,null,null,null,0,?,null,null)");
        statement.setString(1,schema.schemaName);
        statement.setString(2,nullableBTable.tableName);
        statement.setString(3,fullTestFile.getFilePath());
        statement.setString(4,BADDIR.getCanonicalPath());

        ResultSet resultSet = statement.executeQuery();
        //make sure that the bad records list is good
        validateImportResults(resultSet,size,0);

        //make sure that the data matches
        List<int[]> actualData = Lists.newArrayListWithExpectedSize(size);
        ResultSet rs = conn.query("select * from "+ nullableBTable);
        while(rs.next()){
            int[] data = new int[2];
            data[0] = rs.getInt(1);
            Assert.assertFalse("Column a was null!",rs.wasNull());
            data[1] = rs.getInt(2);
            Assert.assertFalse("Column b was null!",rs.wasNull());
            actualData.add(data);
        }

        Collections.sort(correctFullData, intArrayComparator);
        Collections.sort(actualData, intArrayComparator);
        assertCorrectResult(correctFullData,actualData);
    }

    @Test @Ignore("DB-4340")
    public void testUpsertWithEmptyTableWillInsertDuplicatesReportedAsBad() throws Exception {
        //  FIXME: JC - 'Additive WriteConflict'. Terminates connection, later tests fail, no good error message.
        CallableStatement statement =
                conn.prepareCall("call SYSCS_UTIL.UPSERT_DATA_FROM_FILE(?,?,null,?,null,null,null,null,null,0,?,null,null)");
        statement.setString(1,schema.schemaName);
        statement.setString(2,nullableBTable.tableName);
        statement.setString(3,fullTestFileWithDuplicates.getFilePath());
        statement.setString(4,BADDIR.getCanonicalPath());

        ResultSet resultSet = statement.executeQuery();
        //make sure that the bad records list is good
        validateImportResults(resultSet,size,1);

        //make sure that the data matches
        List<int[]> actualData = Lists.newArrayListWithExpectedSize(size);
        ResultSet rs = conn.query("select * from "+ nullableBTable);
        while(rs.next()){
            int[] data = new int[2];
            data[0] = rs.getInt(1);
            Assert.assertFalse("Column a was null!",rs.wasNull());
            data[1] = rs.getInt(2);
            Assert.assertFalse("Column b was null!",rs.wasNull());
            actualData.add(data);
        }

        Collections.sort(correctFullData, intArrayComparator);
        Collections.sort(actualData, intArrayComparator);
        assertCorrectResult(correctFullData,actualData);
    }

    /*******************************************************************************************************************/
    /*helper methods*/

    private void assertCorrectResult(List<int[]> correct,List<int[]> actualData) {
        Assert.assertEquals("Incorrect result size!", correct.size(), actualData.size());
        for(int i=0;i<correct.size();i++){
            int[] corr = correct.get(i);
            int[] actual = actualData.get(i);
            Assert.assertEquals("Incorrect value for column A!",corr[0],actual[0]);
            Assert.assertEquals("Incorrect value for column B!",corr[1],actual[1]);
        }
    }

    private static TestConnection createConnection() throws Exception {
        Connection baseConn = SpliceNetConnection.getConnectionAs(
                SpliceNetConnection.DEFAULT_USER,
                SpliceNetConnection.DEFAULT_USER_PASSWORD);
        TestConnection conn = new TestConnection(baseConn);
        conn.setSchema(schema.schemaName);
        return conn;
    }
}
