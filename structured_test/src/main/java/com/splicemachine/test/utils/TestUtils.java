package com.splicemachine.test.utils;

import com.google.common.collect.Sets;
import com.splicemachine.test.nist.DerbyNistRunner;
import com.splicemachine.test.nist.SpliceNistRunner;
import org.apache.commons.dbutils.DbUtils;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author Jeff Cunningham
 *         Date: 9/16/13
 */
public class TestUtils {

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

    public static void cleanup(DerbyNistRunner derbyRunner, SpliceNistRunner spliceRunner, PrintStream out) throws Exception {
        // TODO: Still not quite working.  See Derby's JDBC.dropSchema(DatabaseMetaData dmd, String schema)
        Connection derbyConnection = derbyRunner.getConnection();
        derbyConnection.setAutoCommit(false);
        ResultSet derbySchema = derbyConnection.getMetaData().getSchemas();
        out.println("Dropping Derby test schema...");
        try {
            dropSchema(derbyConnection, derbySchema, verbotten, out);
        } catch (Exception e) {
            out.println("Dropping Derby schema failed: "+e.getLocalizedMessage());
            e.printStackTrace(out);
        } finally {
            derbyConnection.commit();
            derbyConnection.close();
        }

        Connection spliceConnection = spliceRunner.getConnection();
        spliceConnection.setAutoCommit(false);
        ResultSet spliceSchema = spliceConnection.getMetaData().getSchemas();
        out.println("Dropping Splice test schema...");
        try {
            dropSchema(spliceConnection, spliceSchema, verbotten, out);
        } catch (Exception e) {
            out.println("Dropping Splice schema failed: " + e.getLocalizedMessage());
            e.printStackTrace(out);
        } finally {
            spliceConnection.commit();
            spliceConnection.close();
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
}
