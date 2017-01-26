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

package com.splicemachine.homeless;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.*;
import java.util.*;

import com.splicemachine.db.iapi.types.SQLClob;
import org.spark_project.guava.collect.Lists;
import com.splicemachine.utils.Pair;
import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.io.FileUtils;
import org.junit.runner.Description;

import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;

public class TestUtils {

    public static long baseTableConglomerateId(Connection conn, String schema, String table) throws SQLException{
        schema = schema.toUpperCase();
        table = table.toUpperCase();

        /*
         * This is a needlessly-complicated and annoying way of doing this,
	     * because *when it was written*, the metadata information was kind of all messed up
	     * and doing a join between systables and sysconglomerates resulted in an error. When you are
	     * looking at this code and going WTF?!? feel free to try cleaning up the SQL. If you get a bunch of
	     * wonky errors, then we haven't fixed the underlying issue yet. If you don't, then you just cleaned up
	     * some ugly-ass code. Good luck to you.
	     *
	     */
        try(PreparedStatement ps = conn.prepareStatement("select c.conglomeratenumber from "+
                "sys.systables t, sys.sysconglomerates c,sys.sysschemas s "+
                "where t.tableid = c.tableid "+
                "and s.schemaid = t.schemaid "+
                "and c.isindex = false "+
                "and t.tablename = ? "+
                "and s.schemaname = ?")){
            ps.setString(1,table);
            ps.setString(2,schema);
            try(ResultSet rs=ps.executeQuery()){
                if(rs.next()){
                    long aLong=rs.getLong(1);
                    if(rs.next())
                        throw new IllegalStateException("More than one non-index conglomerate was found for table "+schema+"."+table+"!");
                    return aLong;
                }else{
                    throw new IllegalStateException("No conglomerate found for table "+schema+"."+table);
                }
            }
        }
    }

    public static void executeSql(Connection connection, String sqlStatements, String schema) {
        try {
            String str = sqlStatements.replaceAll("<SCHEMA>", schema);
            str = str.replaceAll("<DIR>",SpliceUnitTest.getResourceDirectory());
            for (String s : str.split(";")) {
                String trimmed = s.trim();
                if (!trimmed.equals("")) {
                    Statement stmt = connection.createStatement();
                    stmt.execute(s);
                    connection.commit();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Error running SQL statements: " + sqlStatements, e);
        }
    }

    public static void executeSqlFile(Connection connection, String fileSuffix, String schema) {
        executeSql(connection, readFile(fileSuffix), schema);
    }

    /* @Deprecated Use the version that takes java.sql.Connection rather than a junit TestWatcher */
    @Deprecated
    public static void executeSqlFile(SpliceWatcher watcher, String fileSuffix, String schema) {
        executeSql(watcher.getOrCreateConnection(), readFile(fileSuffix), schema);
    }

    private static String readFile(String fileName) {
        try {
            File f = new File(SpliceUnitTest.getResourceDirectory(), fileName);
            return FileUtils.readFileToString(f);
        } catch (IOException e) {
            throw new RuntimeException("Unable to open file " + fileName, e);
        }
    }

    private static List<Map> resultSetToOrderedMaps(ResultSet rs) throws SQLException{

        List<Map> results =new ArrayList<>();

        while(rs.next()){
            results.add(resultSetToOrderedMap(rs));
        }

        return results;
    }

    private static Map<String, Object> resultSetToOrderedMap(ResultSet rs) throws SQLException {
        Map<String, Object> result =new LinkedHashMap<>();
        ResultSetMetaData rsmd = rs.getMetaData();
        int cols = rsmd.getColumnCount();

        for (int i = 1; i <= cols; i++) {
            result.put(rsmd.getColumnName(i), rs.getObject(i));
        }

        return result;
    }

    public static List<Map> resultSetToMaps(ResultSet rs) throws SQLException{

        List<Map> results =new ArrayList<>();
        BasicRowProcessor brp = new BasicRowProcessor();

        while(rs.next()){
            results.add(brp.toMap(rs));
        }

        return results;
    }

    public static List<Object[]> resultSetToArrays(ResultSet rs) throws SQLException {
        List<Object[]> results =new ArrayList<>();
        BasicRowProcessor brp = new BasicRowProcessor();

        while (rs.next()){
            results.add(brp.toArray(rs));
        }

        return results;
    }

    /* Make an Object array from args */
    public static Object[] o(Object... o){
        return o;
    }

    public static List<Map> tableLookupByNumber(SpliceWatcher spliceWatcher) throws Exception{
        List<Map> results = tableLookupByNumberNoPrint(spliceWatcher);
        System.out.println("Table ID\t\tConglomerate Number\t\tTable Name");
        for( Map m : results){
            System.out.println(String.format("%s\t\t%s\t\t%s", m.get("TABLEID"), m.get("CONGLOMERATENUMBER"), m.get("TABLENAME")));
        }

        return results;
    }

    public static List<Map> tableLookupByNumberNoPrint(SpliceWatcher spliceWatcher) throws Exception{
        ResultSet rs = spliceWatcher.executeQuery("select t1.tableid, t2.tablename, t1.CONGLOMERATENUMBER " +
                                                      "                                  from sys.sysconglomerates t1, sys.systables t2  " +
                                                      "                                  where t1.tableid=t2.tableid and t2.tablename not like 'SYS%'" +
                                                      "                                  order by t1.conglomeratenumber desc");

        return resultSetToMaps(rs);
    }

    public static String lookupConglomerateNumber(String schemaName, String tableName, SpliceWatcher spliceWatcher) throws Exception {
        ResultSet rs = spliceWatcher.executeQuery("select t1.conglomeratenumber from sys.sysconglomerates t1, " +
                                                      "sys.systables t2, sys.sysschemas t3 where t1.tableid = t2.tableid" +
                                                      " and t2.tablename = '"+tableName.toUpperCase()+"' and t3.schemaname = '"+schemaName.toUpperCase()+"'" +
                                                      "  and t2.schemaid = t1.schemaid and t2.schemaid = t3.schemaid");
        String conglomNum = null;
        if (rs.next()) {
            conglomNum = rs.getString(1);
        }
        return conglomNum;
    }

    public static SpliceDataWatcher createFileDataWatcher(final SpliceWatcher watcher, final String fileName, final String className){
        return new SpliceDataWatcher() {
            @Override
            protected void starting(Description description) {

                try {
                    TestUtils.executeSqlFile(watcher.getOrCreateConnection(), fileName, className);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    public static SpliceDataWatcher createStringDataWatcher(final SpliceWatcher watcher, final String sqlStatements, final String className){
        return new SpliceDataWatcher() {
            @Override
            protected void starting(Description description) {

                try {
                    TestUtils.executeSql(watcher.getOrCreateConnection(), sqlStatements, className);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    /**
     * SQLState fields of SQLExceptions are used for comparing expected exceptions in tests. This is done
     * because the error states are less likely to change than the error messages.<br/>
     * However, SQLState constant keys sometimes have multiple components separated by '.', such as <code>X0X10.S</code>,
     * which are stripped off at runtime.<br/>
     * This method is used by tests to trim these "extra" components off of the keys so that they can be compared to
     * those of the expected exceptions.
     * @param sqlState the SQLState key to which to compare to the actual exception's, i.e.,
     *                 <code>ErrorState.INVALID_COLUMN_NAME.getSqlState()</code>
     * @return the SQLState string with extra components, if any, removed.
     */
    public static String trimSQLState(String sqlState) {
        int indexOfDot = sqlState.indexOf('.');
        if (indexOfDot >= 0) {
            return sqlState.substring(0, indexOfDot);
        }
        return sqlState;
    }

    /**
     * Calculate and return the string duration of the given start and end times (in milliseconds)
     * @param startMilis the starting time of the duration given by <code>System.currentTimeMillis()</code>
     * @param stopMilis the ending time of the duration given by <code>System.currentTimeMillis()</code>
     * @return example <code>0 hrs 04 min 41 sec 337 mil</code>
     */
    public static String getDuration(long startMilis, long stopMilis) {

        long secondInMillis = 1000;
        long minuteInMillis = secondInMillis * 60;
        long hourInMillis = minuteInMillis * 60;

        long diff = stopMilis - startMilis;
        long elapsedHours = diff / hourInMillis;
        diff = diff % hourInMillis;
        long elapsedMinutes = diff / minuteInMillis;
        diff = diff % minuteInMillis;
        long elapsedSeconds = diff / secondInMillis;
        diff = diff % secondInMillis;

        return String.format("%d hrs %02d min %02d sec %03d mil", elapsedHours, elapsedMinutes, elapsedSeconds, diff);
    }

    public static int printResult(String statement, ResultSet rs, PrintStream out) throws SQLException {
        if (rs.isClosed()) {
            return 0;
        }
        int resultSetSize = 0;
        out.println();
        out.println(statement);
        List<Map> maps = TestUtils.resultSetToOrderedMaps(rs);
        if (maps.size() > 0) {
            List<String> keys = new ArrayList<String>(maps.get(0).keySet());
            for (String col : keys) {
                out.print(" "+col+" |");
            }
            out.println();
            for (int i=0; i<keys.size(); ++i) {
                out.print("-----");
            }
            out.println();
            for (Map map : maps) {
                ++resultSetSize;
                for (String key : keys) {
                    out.print(" "+map.get(key)+" |");
                }
                out.println();
            }
        }
        out.println("--------------------");
        out.println(resultSetSize+" rows");
        return resultSetSize;
    }

    public static FormattedResult indexQuery(Connection connection, String schemaName, String tableName) throws Exception {
        String indexQuery = String.format("select t1.tableid, t2.tablename, t1.descriptor, " +
                                              "t1.CONGLOMERATENUMBER from sys.sysconglomerates t1, " +
                                              "sys.systables t2, sys.sysschemas t3 where t1.tableid=t2.tableid " +
                                              "and t2.schemaid=t3.schemaid and t3.schemaname = '%s' and t2" +
                                              ".tablename = '%s' order by t1.conglomeratenumber desc",
                                          schemaName, tableName);
        ResultSet rs = connection.createStatement().executeQuery(indexQuery);
        return FormattedResult.ResultFactory.convert(indexQuery, rs);
    }

    /**
     * Create expected and actual results and compare them. Use JUnit's differencing
     * ("Click to see the difference") to visually compare results.
     * <p>
     *  This class was created to make it easier to compare results from another SQL tool
     *  like Derby ij, sqlfiddle.com to a Splice result (ResultSet). Expected results can be easily
     *  copied into a list.<br/>
     *  @see ResultFactory#convert(String, java.sql.ResultSet)
     *  @see ResultFactory#convert(String, String, java.util.List, String)
     * </p>
     * <p>
     *  Comparision is done by "compacting" the column names (removing any whitespace) and
     *  comparing as is, then all rows are compacted, sorted and compared.
     * </p>
     */
    public static class FormattedResult {
        private final String query;
        private final List<String> columns;
        private final List<List<String>> rows;
        private final boolean sort;

        private FormattedResult(String query, List<String> columns, List<List<String>> rows, boolean sort) {
            this.query = query;
            this.columns = columns;
            this.rows = rows;
            this.sort = sort;
        }

        public int size() {
            return this.rows.size();
        }

        @Override
        public String toString() {
            Map<Integer,Integer> colWidth = new HashMap<Integer, Integer>(this.columns.size());
            StringBuilder buf = new StringBuilder(query);
            buf.append("\n");
            int totalLength = 0;
            for (int i=0; i<this.columns.size(); ++i) {
                String col = this.columns.get(i);
                int maxColWidth = getMaxWidth(col.length(), i);
                colWidth.put(i, maxColWidth);
                Pair<String,String> pad = getPad(col.length(), maxColWidth);
                buf.append(pad.getFirst()).append(col).append(pad.getSecond()).append('|');
                totalLength += pad.getFirst().length() + col.length() + pad.getSecond().length() + 1;
            }
            buf.append("\n");
            for (; totalLength>0; totalLength--) {
                buf.append('-');
            }
            buf.append("\n");

            List<List<String>> sortedRows = Lists.newArrayList(rows);
            if(sort) {
                Collections.sort(sortedRows, new ListComparator());
            }
            for (List<String> row : sortedRows) {
                int i=0;
                for (String colVal : row) {
                    Pair<String,String> pad = getPad(colVal.length(), colWidth.get(i++));
                    buf.append(pad.getFirst()).append(colVal).append(pad.getSecond()).append('|');
                }
                buf.append("\n");
            }
            buf.append("\n");
            return buf.toString();
        }

        private Pair<String,String> getPad(int length, int maxLength) {
            int frontPadLength = (maxLength/2) - (length/2);
            StringBuilder pad = new StringBuilder();
            for (int i=0; i<frontPadLength; i++) {
                pad.append(' ');
            }
            String front = pad.toString();
            pad.setLength(0);
            for (int i=0; i<=(maxLength-(frontPadLength+length)); i++) {
                pad.append(' ');
            }
            String back = pad.toString();
            return new Pair<String, String>(front,back);
        }

        private int getMaxWidth(int min, int colIndex) {
            int maxWidth = (min % 2 == 0 ? min : min +1);
            for (List<String> row : this.rows) {
                String colVal = row.get(colIndex);
                maxWidth = Math.max(maxWidth,colVal.length());
            }
            return maxWidth;
        }

        public static class ResultFactory {

            /**
             * Converts expected results to a <code>FormattedResult</code>
             *
             * @param query the query string
             * @param columnStr the string of column names separated by <code>columnSeparator</code> regex.
             *                  These are taken "as is" and will not be sorted or otherwise modified.
             * @param rows the list of strings of row column values separated by <code>columnSeparator</code> regex.
             *             These rows will be sorted for comparison.
             * @param columnSeparator regex on which to parse the column and row strings.   @return FormattedResult
             */
            public static FormattedResult convert(String query, String columnStr, List<String> rows, String columnSeparator) {
                List<String> columns = new ArrayList<String>();
                List<List<String>> rowKeyToRows = new ArrayList<List<String>>();
                columns.addAll(Arrays.asList(columnStr.split(columnSeparator)));

                for (String rowStr : rows) {
                    List<String> row = new ArrayList<String>();
                    for (String columnValue : rowStr.split(columnSeparator)) {
                        row.add((columnValue.contains("(null)") ? "NULL" : columnValue.trim()));
                    }
                    rowKeyToRows.add(row);
                }
                return new FormattedResult(query, columns, rowKeyToRows, true);
            }

            /**
             * Converts actual results to a <code>FormattedResult</code>
             *
             * @param query the query string
             * @param rs the JDBC ResultSet to convert.  ResultSet rows will be sorted for comparison.
             * @return FormattedResult
             * @throws Exception if there a problem with the ResultSet.
             */
            public static FormattedResult convert(String query, ResultSet rs) throws Exception {
                return convert(query, rs, true);
            }

            /**
             * Create a FormattedResult.  Set sort = false to NOT sort the rows in the output string.  Do this if
             * the ResultSet you are verifying is already ordered and thus should be the same every time.
             */
            public static FormattedResult convert(String query, ResultSet rs, boolean sort) throws Exception {
                List<String> columns = new ArrayList<String>();
                List<List<String>> rows = new ArrayList<List<String>>();
                ResultSetMetaData metaData = rs.getMetaData();
                int nCols = metaData.getColumnCount();

                boolean gotColumnNames = false;
                while (rs.next()) {
                    List<String> row = new ArrayList<String>();
                    for (int i = 1; i <= nCols; i++) {
                        if (! gotColumnNames) {
                            columns.add(metaData.getColumnName(i).trim());
                        }
                        Object value = rs.getObject(i);
                        if (value != null && value instanceof Clob) {
                            Clob clob = (Clob) value;
                             row.add( clob.getSubString(1,(int)clob.length()));
                        } else {
                            row.add((value != null && value.toString() != null ? value.toString().trim() : "NULL"));
                        }
                    }
                    rows.add(row);
                    gotColumnNames = true;
                }
                return new FormattedResult(query, columns, rows, sort);
            }

            /**
             * Convert the ResultSet to a FormattedResult and return the trimmed string version of that
             * with rows sorted lexicographically
             */
            public static String toString(ResultSet rs) throws Exception {
                return convert("", rs).toString().trim();
            }

            /**
             * Convert the ResultSet to a FormattedResult and return the trimmed string version of that
             * with rows unsorted
             */
            public static String toStringUnsorted(ResultSet rs) throws Exception {
                return convert("", rs, false).toString().trim();
            }

        }

    }

    private static class ListComparator implements Comparator<List<String>> {
        @Override
        public int compare(List<String> list1, List<String> list2) {
            for (int i = 0; i < list1.size(); i++) {
                int c = list1.get(i).compareTo(list2.get(i));
                if (c != 0) {
                    return c;
                }
            }
            return 0;
        }
    }

}
