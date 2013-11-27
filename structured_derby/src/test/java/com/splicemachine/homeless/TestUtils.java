package com.splicemachine.homeless;

import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.io.IOUtils;
import org.junit.runner.Description;

public class TestUtils {

    public static URL getClasspathResource(String path){
        return TestUtils.class.getClassLoader().getResource(path);
    }

    public static void executeSql(SpliceWatcher spliceWatcher, String sqlStatements, String schema){
        try {
            for (String s : sqlStatements.replaceAll("<SCHEMA>", schema).split(";")){
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

    public static String readFile(String fileName) {
        try {
            File f = new File(fileName);
            URL pathToFile = null;

            if (f.isAbsolute()) {
                pathToFile = f.toURL();

            } else {
                pathToFile = getClasspathResource(fileName);
            }

            return IOUtils.toString(pathToFile);

        } catch (IOException e) {
            throw new RuntimeException("Unable to open file " + fileName, e);
        }
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
        ResultSet rs = spliceWatcher.executeQuery("select t1.tableid, t2.tablename, t1.CONGLOMERATENUMBER " +
                "                                  from sys.sysconglomerates t1, sys.systables t2  " +
                "                                  where t1.tableid=t2.tableid and t2.tablename not like 'SYS%'" +
                "                                  order by t1.conglomeratenumber desc");

        List<Map> results = resultSetToMaps(rs);
        System.out.println("Table ID\t\tConglomerate Number\t\tTable Name");
        for( Map m : results){
            System.out.println(String.format("%s\t\t%s\t\t%s", m.get("TABLEID"), m.get("CONGLOMERATENUMBER"), m.get("TABLENAME")));
        }

        return results;
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
        List<Map> maps = TestUtils.resultSetToMaps(rs);
        if (maps.size() > 0) {
            List<String> keys = new ArrayList<String>(maps.get(0).keySet());
            Collections.sort(keys);
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

}
