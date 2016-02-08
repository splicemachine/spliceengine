package com.splicemachine.derby.test.framework;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.runner.Description;

import com.splicemachine.utils.Pair;

import static org.junit.Assert.assertTrue;

public class SpliceUnitTest {

    private static Pattern overallCostP = Pattern.compile("totalCost=[0-9]+\\.?[0-9]*");

	public String getSchemaName() {
		Class<?> enclosingClass = getClass().getEnclosingClass();
		if (enclosingClass != null)
		    return enclosingClass.getSimpleName().toUpperCase();
		else
		    return getClass().getSimpleName().toUpperCase();
	}

    /**
     * Load a table with given values
     *
     * @param statement calling test's statement that may be in txn
     * @param tableName fully-qualified table name, i.e., <pre>schema.table</pre>
     * @param values list of row values
     * @throws Exception
     */
    public static void loadTable(Statement statement, String tableName, List<String> values) throws Exception {
        for (String rowVal : values) {
            statement.executeUpdate("insert into " + tableName + " values " + rowVal);
        }
    }

    public String getTableReference(String tableName) {
		return getSchemaName() + "." + tableName;
	}

	public String getPaddedTableReference(String tableName) {
		return " " + getSchemaName() + "." + tableName.toUpperCase()+ " ";
	}

	
	public static int resultSetSize(ResultSet rs) throws Exception {
		int i = 0;
		while (rs.next()) {
			i++;
		}
		return i;
	}

    public static int columnWidth(ResultSet rs ) throws SQLException {
        return rs.getMetaData().getColumnCount();
    }

	public static String format(String format, Object...args) {
		return String.format(format, args);
	}
	public static String getBaseDirectory() {
		String userDir = System.getProperty("user.dir");
	    if(!userDir.endsWith("engine_it"))
	    	userDir = userDir+"/engine_it";
	    return userDir;
	}

    public static String getResourceDirectory() {
		return getBaseDirectory()+"/src/test/test-data/";
	}

    public static String getHBaseDirectory() {
		return getBaseDirectory()+"/target/hbase";
	}
    
    public static String getHiveWarehouseDirectory() {
		return getBaseDirectory()+"/user/hive/warehouse";
	}

    public static class MyWatcher extends SpliceTableWatcher {

        public MyWatcher(String tableName, String schemaName, String createString) {
            super(tableName, schemaName, createString);
        }

        public void create(Description desc) {
            super.starting(desc);
        }
    }

    protected void firstRowContainsQuery(String query, String contains,SpliceWatcher methodWatcher) throws Exception {
        rowContainsQuery(1,query,contains,methodWatcher);
    }

    protected void secondRowContainsQuery(String query, String contains,SpliceWatcher methodWatcher) throws Exception {
        rowContainsQuery(2,query,contains,methodWatcher);
    }

    protected void thirdRowContainsQuery(String query, String contains,SpliceWatcher methodWatcher) throws Exception {
        rowContainsQuery(3,query,contains,methodWatcher);
    }

    protected void fourthRowContainsQuery(String query, String contains,SpliceWatcher methodWatcher) throws Exception {
        rowContainsQuery(4,query,contains,methodWatcher);
    }

    protected void rowContainsQuery(int[] levels, String query,SpliceWatcher methodWatcher,String... contains) throws Exception {
        try(ResultSet resultSet = methodWatcher.executeQuery(query)){
            int i=0;
            int k=0;
            while(resultSet.next()){
                System.out.println(resultSet.getString(1));
                i++;
                for(int level : levels){
                    if(level==i){
                        Assert.assertTrue("failed query: "+query+" -> "+resultSet.getString(1),resultSet.getString(1).contains(contains[k]));
                        k++;
                    }
                }
            }
        }
    }

    protected void rowContainsQuery(int level, String query, String contains, SpliceWatcher methodWatcher) throws Exception {
        ResultSet resultSet = methodWatcher.executeQuery(query);
        for (int i = 0; i< level;i++) {
            resultSet.next();
        }
        String actualString = resultSet.getString(1);
        String failMessage = String.format("expected result of query '%s' to contain '%s' at row %,d but did not, actual result was '%s'",
                query, contains, level, actualString);
        Assert.assertTrue(failMessage, actualString.contains(contains));
    }

    protected void queryDoesNotContainString(String query, String notContains,SpliceWatcher methodWatcher) throws Exception {
        ResultSet resultSet = methodWatcher.executeQuery(query);
        while (resultSet.next())
            Assert.assertFalse("failed query: " + query + " -> " + resultSet.getString(1), resultSet.getString(1).contains(notContains));
    }

    protected void rowManyContainsQuery(int level, String query, SpliceWatcher methodWatcher,String... contains) throws Exception {
        ResultSet resultSet = methodWatcher.executeQuery(query);
        for (int i = 0; i< level;i++)
            resultSet.next();
        for (String contain: contains)
            Assert.assertTrue("failed query: " + query + " -> " + resultSet.getString(1),resultSet.getString(1).contains(contain));
    }

    public static void rowsContainsQuery(String query, Contains mustContain, SpliceWatcher methodWatcher) throws Exception {
        ResultSet resultSet = methodWatcher.executeQuery(query);
        int i = 0;
        for (Pair<Integer,String> p : mustContain.get()) {
            for (; i< p.getFirst();i++)
                resultSet.next();
            Assert.assertTrue("failed query: " + query + " -> " + resultSet.getString(1), resultSet.getString(1).contains(p.getSecond()));
        }
    }


    public static double parseTotalCost(String planMessage) {
        Matcher m1 = overallCostP.matcher(planMessage);
        Assert.assertTrue("No Overall cost found!", m1.find());
        return Double.parseDouble(m1.group().substring("totalCost=".length()));
    }

    public static class Contains {
        private List<Pair<Integer,String>> rows = new ArrayList<>();

        public Contains add(Integer row, String shouldContain) {
            rows.add(new Pair<>(row, shouldContain));
            return this;
        }

        public List<Pair<Integer,String>> get() {
            Collections.sort(this.rows, new Comparator<Pair<Integer, String>>() {
                @Override
                public int compare(Pair<Integer, String> p1, Pair<Integer, String> p2) {
                    return p1.getFirst().compareTo(p2.getFirst());
                }
            });
            return this.rows;
        }
    }

    protected static void importData(SpliceWatcher methodWatcher, String schema,String tableName, String fileName) throws Exception {
        String file = SpliceUnitTest.getResourceDirectory()+ fileName;
        PreparedStatement ps = methodWatcher.prepareStatement(String.format("call SYSCS_UTIL.IMPORT_DATA('%s','%s','%s','%s',',',null,null,null,null,1,null,true,'utf-8')", schema, tableName, null, file));
        ps.executeQuery();
    }

    protected static void validateImportResults(ResultSet resultSet, int good,int bad) throws SQLException {
        Assert.assertTrue("No rows returned!",resultSet.next());
        Assert.assertEquals("Incorrect number of files reported!",1,resultSet.getInt(3));
        Assert.assertEquals("Incorrect number of rows reported!",good,resultSet.getInt(1));
        Assert.assertEquals("Incorrect number of bad records reported!", bad, resultSet.getInt(2));
    }

    public static String printMsgSQLState(String testName, SQLException e) {
        // useful for debugging import errors
        StringBuilder buf =new StringBuilder(testName);
        buf.append("\n");
        int i =1;
        SQLException child = e;
        while (child != null) {
            buf.append(i++).append(" ").append(child.getSQLState()).append(" ")
                    .append(child.getLocalizedMessage()).append("\n");
            child = child.getNextException();
        }
        return buf.toString();
    }

    public static File createBadLogDirectory(String schemaName) {
        File badImportLogDirectory = new File(SpliceUnitTest.getBaseDirectory()+"/target/BAD/"+schemaName);
        if (badImportLogDirectory.exists()) {
            //noinspection ConstantConditions
            for (File file : badImportLogDirectory.listFiles()) {
                assertTrue("Couldn't create "+file,file.delete());
            }
            assertTrue("Couldn't create "+badImportLogDirectory,badImportLogDirectory.delete());
        }
        assertTrue("Couldn't create "+badImportLogDirectory,badImportLogDirectory.mkdirs());
        assertTrue("Failed to create "+badImportLogDirectory,badImportLogDirectory.exists());
        return badImportLogDirectory;
    }

}