package com.splicemachine.homeless;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import com.google.common.collect.Lists;
import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.runner.Description;

import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;

public class TestUtils {

    public static URL getClasspathResource(String path){
        return TestUtils.class.getClassLoader().getResource(path);
    }

    public static void executeSql(SpliceWatcher spliceWatcher, String sqlStatements, String schema){
        try {
            String str = sqlStatements.replaceAll("<SCHEMA>",schema);
            str = str.replaceAll("<DIR>",SpliceUnitTest.getResourceDirectory());
            for (String s : str.split(";")){
                String trimmed = s.trim();
                if (!trimmed.equals("")){
                    Statement stmt = spliceWatcher.getStatement();
                    stmt.execute(s);
                    spliceWatcher.commit();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Error running SQL statements: " + sqlStatements, e);
        }
    }


    public static void executeSqlFile(SpliceWatcher spliceWatcher, String fileSuffix, String schema){
        executeSql(spliceWatcher, readFile(fileSuffix), schema);
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

        List<Map> results = new ArrayList<Map>();

        while(rs.next()){
            results.add(resultSetToOrderedMap(rs));
        }

        return results;
    }

    private static Map<String, Object> resultSetToOrderedMap(ResultSet rs) throws SQLException {
        Map<String, Object> result = new LinkedHashMap<String, Object>();
        ResultSetMetaData rsmd = rs.getMetaData();
        int cols = rsmd.getColumnCount();

        for (int i = 1; i <= cols; i++) {
            result.put(rsmd.getColumnName(i), rs.getObject(i));
        }

        return result;
    }

    public static List<Map> resultSetToMaps(ResultSet rs) throws SQLException{

        List<Map> results = new ArrayList<Map>();
        BasicRowProcessor brp = new BasicRowProcessor();

        while(rs.next()){
            results.add(brp.toMap(rs));
        }

        return results;
    }

    public static List<Object[]> resultSetToArrays(ResultSet rs) throws SQLException {
        List<Object[]> results = new ArrayList<Object[]>();
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

    public static SpliceDataWatcher createFileDataWatcher(final SpliceWatcher watcher, final String fileName, final String className){
        return new SpliceDataWatcher() {
            @Override
            protected void starting(Description description) {

                try {
                    TestUtils.executeSqlFile(watcher, fileName, className);
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
                    TestUtils.executeSql(watcher, sqlStatements, className);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
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
                        row.add((value != null ? value.toString().trim() : "NULL"));
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
