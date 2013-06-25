package com.splicemachine.homeless;

import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.io.IOUtils;
import org.junit.runner.Description;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestUtils {

    public static URL getClasspathResource(String path){
        return TestUtils.class.getClassLoader().getResource(path);
    }

    public static void executeSqlFile(SpliceWatcher spliceWatcher, String fileSuffix, String schema){

        String sqlStatementStrings = null;
        try {
            File f = new File(fileSuffix);
            URL pathToFile = null;

            if(f.isAbsolute()){
                pathToFile = f.toURL();

            }else{
                pathToFile = getClasspathResource(fileSuffix);
            }

            sqlStatementStrings = IOUtils.toString(pathToFile);

        } catch (IOException e) {
            throw new RuntimeException("Unable to open file " + fileSuffix, e);
        }

        try {
            for (String s : sqlStatementStrings.replaceAll("<SCHEMA>", schema).split(";")){
                String trimmed = s.trim();
                if (!trimmed.equals("")){
                    Statement stmt = spliceWatcher.getStatement();
                    stmt.execute(s);
                    spliceWatcher.commit();
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Error loading SQL file at path : " + fileSuffix, e);
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
}
