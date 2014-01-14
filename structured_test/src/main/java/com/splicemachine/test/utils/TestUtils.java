package com.splicemachine.test.utils;

import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.splicemachine.test.runner.TestRunner;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.derby.tools.ij;
import org.junit.Assert;

import java.io.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author Jeff Cunningham
 *         Date: 9/16/13
 */
public class TestUtils {

	/**
	 * File filter to use when determining types of files to be included in a list.
	 */
	public static class SpliceIOFileFilter implements IOFileFilter {
		private final List<String> inclusions;
		private final List<String> exclusions;
		public SpliceIOFileFilter(List<String> inclusions, List<String> exclusions) {
			this.inclusions = inclusions;
			this.exclusions = exclusions;
		}
	
		@Override
		public boolean accept(File file) {
			if (inclusions != null) {
                return inclusions.contains(file.getName());
			}
			if (exclusions != null) {
				if (exclusions.contains(file.getName()))
					return false;
			}
			return true;
		}
	
		@Override
		public boolean accept(File dir, String name) {
			// only accepting files
			return false;
		}
		
	}

	public static int DEFAULT_THREAD_POOL_SIZE = 4;

	public static final String DERBY_OUTPUT_EXT = ".derby";
	public static final String SPLICE_OUTPUT_EXT = ".splice";
	public static final String SQL_FILE_EXT = ".sql";

    private static final String ALL_TABLES_QUERY = "SELECT SYS.SYSSCHEMAS.SCHEMANAME, SYS.SYSTABLES.TABLETYPE," +
            " SYS.SYSTABLES.TABLENAME, SYS.SYSTABLES.TABLEID FROM SYS.SYSTABLES INNER JOIN SYS.SYSSCHEMAS" +
            " ON (SYS.SYSTABLES.SCHEMAID = SYS.SYSSCHEMAS.SCHEMAID) WHERE SYS.SYSSCHEMAS.SCHEMANAME = '%s'";

    private static final String DEPENDENCY_QUERY = "SELECT SYS.SYSSCHEMAS.SCHEMANAME, SYS.SYSTABLES.TABLETYPE," +
            " SYS.SYSTABLES.TABLENAME, SYS.SYSTABLES.TABLEID, SYS.SYSDEPENDS.DEPENDENTID, SYS.SYSDEPENDS.PROVIDERID" +
            " FROM SYS.SYSDEPENDS INNER JOIN SYS.SYSTABLES ON (SYS.SYSDEPENDS.DEPENDENTID = SYS.SYSTABLES.TABLEID)" +
            " OR (SYS.SYSDEPENDS.PROVIDERID = SYS.SYSTABLES.TABLEID) INNER JOIN SYS.SYSSCHEMAS" +
            " ON (SYS.SYSTABLES.SCHEMAID = SYS.SYSSCHEMAS.SCHEMAID) WHERE SYS.SYSSCHEMAS.SCHEMANAME = '%s'";

    private static final Set<String> verbotten =
            Sets.newHashSet("SYS", "APP", "NULLID", "SQLJ", "SYSCAT", "SYSCS_DIAG", "SYSCS_UTIL", "SYSFUN", "SYSIBM", "SYSPROC", "SYSSTAT");
    
    public static void cleanup(List<TestRunner> runners, PrintStream out) throws Exception {
        // TODO: Still not quite working.  See Derby's JDBC.dropSchema(DatabaseMetaData dmd, String schema)
        for (TestRunner runner : runners) {
            Connection derbyConnection = runner.getConnection();
            derbyConnection.setAutoCommit(false);
            ResultSet derbySchema = derbyConnection.getMetaData().getSchemas();
            out.println(runner.getName()+" - Dropping Derby test schema...");
            try {
                dropSchema(derbyConnection, derbySchema, verbotten, out);
            } catch (Exception e) {
                out.println(runner.getName()+" - Dropping schema failed: "+e.getLocalizedMessage());
                e.printStackTrace(out);
            } finally {
                derbyConnection.commit();
                derbyConnection.close();
            }
        }
    }

    public static ResultSet runQuery(Connection connection, String query, PrintStream out) throws Exception {
        ResultSet rs = null;
        try {
            rs = connection.createStatement().executeQuery(query);
        } catch (SQLException e) {
            out.println("Error executing query: " + query + ". " + e.getLocalizedMessage());
        } finally {
            connection.commit();
        }
        return rs;
    }

    public static void fillDependents(ResultSet rs, DependencyTree tree) throws Exception {
        while (rs.next()) {
            DependencyTree.DependencyNode node =
                    new DependencyTree.DependencyNode(rs.getString("TABLENAME"),
                            rs.getString("TABLEID"),
                            rs.getString("TABLETYPE"),
                            rs.getString("PROVIDERID"),
                            rs.getString("DEPENDENTID"));
            tree.addNode(node);
        }
    }

    public static void fillIndependents(ResultSet rs, DependencyTree tree) throws Exception {
        while (rs.next()) {
            DependencyTree.DependencyNode node =
                    new DependencyTree.DependencyNode(rs.getString("TABLENAME"),
                            rs.getString("TABLEID"),
                            rs.getString("TABLETYPE"),
                            null,
                            null);
            tree.addNode(node);
        }
    }

    public static DependencyTree getTablesAndViews(Connection connection, String schemaName, PrintStream out) throws Exception {
        DependencyTree dependencyTree = new DependencyTree();
        // fill tree with tables/views that have dependencies on each other
        fillDependents(runQuery(connection, String.format(DEPENDENCY_QUERY, schemaName), out), dependencyTree);
        // file tree with independent tables
        fillIndependents(runQuery(connection, String.format(ALL_TABLES_QUERY, schemaName), out), dependencyTree);
        return dependencyTree;
    }

    public static void dropSchema(Connection connection, ResultSet schemaMetadata, Set<String> verbotten, PrintStream out) throws Exception {
        while (schemaMetadata.next()) {
            String schemaName = schemaMetadata.getString("TABLE_SCHEM");
            if (! verbotten.contains(schemaName)) {
                out.println(" Drop Schema: "+schemaName);

                DependencyTree dependencyTree = getTablesAndViews(connection, schemaName, out);
                dropTableOrView(connection, schemaName, dependencyTree.getDependencyOrder(), out);

                Statement statement = null;
                try {
                    statement = connection.createStatement();
                    statement.execute("drop schema " + schemaName + " RESTRICT");
                    connection.commit();
                    out.println(" Dropped Schema: "+schemaName);
                } catch (SQLException e) {
                   out.println("Failed to drop schema "+schemaName+". "+e.getLocalizedMessage());
                } finally {
                    DbUtils.closeQuietly(statement);
                }
            }
        }
    }

    public static List<String> dropTableOrView(Connection connection,
                                                String schemaName,
                                                List<DependencyTree.DependencyNode> nodes,
                                                PrintStream out) {
        List<String> successes = new ArrayList<String>(nodes.size());
        for (DependencyTree.DependencyNode node : nodes) {
            String tableOrView = (node.type.equals("V")?"VIEW": "TABLE");
            Statement statement = null;
            try {
                statement = connection.createStatement();
                String stmt = String.format("drop %s %s.%s",tableOrView,schemaName.toUpperCase(),node.name);
//                String stmt = String.format("drop %s %s",tableOrView,node.name);
                out.println("    Drop: "+stmt);
                statement.execute(stmt);
                connection.commit();
                successes.add(node.name);
            } catch (Exception e) {
                out.println("error dropping "+tableOrView+": " + e.getMessage());
            } finally {
                DbUtils.closeQuietly(statement);
            }
        }
        return successes;
    }

	/**
	 * The workhorse of test execution.
	 * <p>
	 *     This method, called by the test runners, executes a single script against a
	 *     database connection using Derby's ij execution framework.
	 * </p>
	 * @param file the SQL script file to run
	 * @param outputFileExtension the output file extension indicating the type of test runner executing
	 *             the test - one of {@link TestUtils#DERBY_OUTPUT_EXT} or {@link TestUtils#SPLICE_OUTPUT_EXT}.<br/>
	 *                            ij will write test output to a file with the SQL script base name and
	 *                            this extension.
	 * @param outputDirectory the directory in which to place the output file.
	 * @param connection the connection to execute the script against. This will either be an
	 *                   embedded derby connection or a networked splice connection.
	 * @throws Exception thrown upon any error condition. The file input, output as well as the connection
	 * is closed.
	 */
	@SuppressWarnings("ResultOfMethodCallIgnored")
    public static void runTest(File file, String outputFileExtension, String outputDirectory, Connection connection) throws Exception {
	    FileInputStream fis = null;
	    FileOutputStream fop = null;
	    try {
	        fis = new FileInputStream(file);
	        File targetFile = new File(TestUtils.getBaseDirectory()+ outputDirectory + 
	        		file.getName().replace(TestUtils.SQL_FILE_EXT, outputFileExtension));
	        Files.createParentDirs(targetFile);
	        if (targetFile.exists())
	            targetFile.delete();
	        targetFile.createNewFile();
	        fop = new FileOutputStream(targetFile);
	        ij.runScript(connection, fis,"UTF-8",fop,"UTF-8");
	        fop.flush();
	    } catch (Exception e) {
	        try {
	            connection.close();
	        } catch (SQLException e1) {
	            // ignore
	        }
	        throw e;
	    } finally {
	        Closeables.closeQuietly(fop);
	        Closeables.closeQuietly(fis);
	    }
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

	/**
	 * Get the resource directory (&lt;projectBaseDirectory&gt;/src/test/resources)
	 * for this project (structured_test)
	 * @return the full path of the project resource directory
	 */
	public static String getResourceDirectory() {
		return TestUtils.getBaseDirectory()+"/src/test/resources";
	}

	/**
	 * Get the base directory of this (structure_test) project
	 * @return the full path of the project base directory
	 */
	public static String getBaseDirectory() {
		String userDir = System.getProperty("user.dir");
	    if(!userDir.endsWith("structured_test"))
	    	userDir = userDir+"/structured_test";
	    return userDir;
	}

	/**
	 * Returns <code>true</code> if a the given string starts with the
	 * <code>commentPattern</code>
	 *
	 * @param line the string to consider with leading whitespace ignored
	 * @param commentPattern the comment pattern to employ
	 * @return <code>true</code> if and only if the line begins with
	 * the <code>commentPattern</code>
	 */
	private static boolean lineIsComment(String line, String commentPattern) {
        return !(commentPattern == null || commentPattern.isEmpty()) && line.trim().startsWith(commentPattern);
    }

	/**
	 * Read files into a list of Strings optionally ignoring comment lines.
	 *
	 * @param filePath the full path of the file to read.
	 * @param commentPattern the optional beginning line comment to
	 *                       ignore.
	 * @return the list of lines from the file with any comment lines optionally
	 * absent.
	 */
	public static List<String> fileToLines(String filePath, String commentPattern) {
	    List<String> lines = new LinkedList<String>();
	    BufferedReader in = null;
	    try {
	        in = new BufferedReader(new FileReader(filePath));
	
	        String line = in.readLine();
	        while(line != null) {
	            if (commentPattern != null) {
	                if (! lineIsComment(line, commentPattern)) {
	                lines.add(line);
	                }
	            } else {
	                lines.add(line);
	            }
	            line = in.readLine();
	        }
	    } catch (IOException e) {
	       Assert.fail("Unable to read: " + filePath+": "+e.getLocalizedMessage());
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

	/**
	 * Write the given <code>content</code> to the given <code>logName</code>
	 * to be created in the given <code>dirName</code>.
	 * @param dirName full path to the directory in which to create the log.
	 * @param logName the name of the log file.
	 * @param discriminator a file discriminator to append to <code>logName</code>
	 *                      when writing multiple copies of the same file. Ignored
	 *                      if <code>null</code>.
	 * @param content the content of which to write to the file.
	 * @param append if <code>true</code>, append to the file, else delete it.
	 * @throws Exception any failure.
	 */
	@SuppressWarnings("ResultOfMethodCallIgnored")
    public static void createLog(String dirName, String logName, String discriminator, String content, boolean date, boolean append) throws Exception {
	    File targetFile = new File(dirName,logName+(discriminator != null ? discriminator : ""));
	    Files.createParentDirs(targetFile);
	    if (! append) {
			if (targetFile.exists())
				targetFile.delete();
			targetFile.createNewFile();
		}
        if (date) {
            FileUtils.writeStringToFile(targetFile, df.format(new Date())+"\n"+content, append);
        } else {
            FileUtils.writeStringToFile(targetFile, content, append);
        }
    }
    private static final DateFormat df = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy");

    /**
     * Execute a set of SQL scripts in the given test runners.
     * @param testScripts the suite of SQL scripts to run
     * @param runners the list of test runners to execute using the scripts
     * @param log the location to which to print the output.
     * @throws Exception any failure
     */
    public static void runTests(List<File> testScripts, List<TestRunner> runners, PrintStream log) throws Exception {
        log.println("Starting...");
        // print to stdout also for user feedback...
        System.out.println("Starting...");

        for (TestRunner runner : runners) {
            // run all scripts with runner
            log.println(runner.getName()+"...");
            System.out.println(runner.getName()+"...");

            log.println("    Running "+testScripts.size()+" tests...");
            System.out.println("    Running "+testScripts.size()+" tests...");
            long start = System.currentTimeMillis();
            runner.run(testScripts);
            String done = "    Duration: " + getDuration(start, System.currentTimeMillis());
            log.println(done);
            System.out.println(done);
        }
    }
}
