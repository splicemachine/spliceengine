package com.splicemachine.db.impl.tools.ij;

import com.splicemachine.db.iapi.tools.i18n.LocalizedResource;
import com.splicemachine.db.shared.common.sql.Utils;
import com.splicemachine.db.tools.JDBCDisplayUtil;
import com.splicemachine.db.impl.tools.ij.ijResultSetResult.ColumnParameters;
import com.sun.org.apache.xpath.internal.operations.Bool;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.lang.reflect.*;
import java.sql.*;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// database resources are closed by caller
@SuppressFBWarnings({"NM_CLASS_NAMING_CONVENTION", "ODR_OPEN_DATABASE_RESOURCE", "OBL_UNSATISFIED_OBLIGATION"})
public class ijCommands {
    public Connection theConnection;
    public Connection theConnection2;
    public ConnectionEnv currentConnEnv;

    Boolean serverLikeFix = null;
    String getVersion() throws SQLException {
        Statement stmt = theConnection.createStatement();
        try (ResultSet rs = stmt.executeQuery("CALL SYSCS_UTIL.SYSCS_GET_VERSION_INFO()" ) ) {
            rs.next();
            return rs.getString(2);
        }
    }

    static public int[] parseVersion(String v) {
        Pattern r = Pattern.compile("(\\d*)\\.(\\d*)\\.(\\d)*\\.(\\d*)(-.*)?");
        Matcher m = r.matcher(v);
        if( m.find() ) {
            return new int[] {Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)),
                    Integer.parseInt(m.group(3)), Integer.parseInt(m.group(4))};
        }
        else
            return null;
    }

    // see DB-11101
    boolean hasServerLikeFix() {
        if(serverLikeFix == null) {
            try {
                int[] v = parseVersion(getVersion());
                if(v != null && v[0] >= 3 && v[3] >= 1988 ){
                    serverLikeFix = Boolean.TRUE;
                }
                else
                    serverLikeFix = Boolean.FALSE;
            } catch (Exception e) {
                serverLikeFix = Boolean.FALSE;
            }
        }
        return serverLikeFix;
    }

    ijCommands() {}
    ijCommands(Connection theConnection, ConnectionEnv currentConnEnv)
    {
        this.theConnection = theConnection;
        this.currentConnEnv = currentConnEnv;
    }

    void haveConnection() {
        JDBCDisplayUtil.checkNotNull(theConnection, "connection");
    }
    /**
     * Returns a subset of the input integer array
     *
     * @param input The input integer array
     * @param start Starting index, inclusive
     * @param end   Ending index, exclusive
     */
    public static ColumnParameters[] arraySubset(final ColumnParameters[] input, int start, int end) {
        ColumnParameters[] res = new ColumnParameters[end-start];
        System.arraycopy(input, start, res, 0, end-start);
        return res;
    }

    public String getRunningOperation(String command) throws SQLException {
        ResultSet rs = null;
        if(theConnection2 == null) return "no";
        try {
            haveConnection();

            rs = theConnection2.createStatement().executeQuery
                    ("call SYSCS_UTIL.SYSCS_GET_RUNNING_OPERATIONS()");
            while(rs.next()) {
                if (rs.getString(5).equals(command))
                    return rs.getString(9);
            }
            return "";
        } catch (SQLException e) {
            try {
                if (rs != null)
                    rs.close();
            } catch (SQLException se) {
            }
            throw e;
        }
    }

    /**
     * Return a resultset of tables (or views, procs...) in the given schema.
     * @param schema  Schema to get tables for, or null for searchin all schemas.
     * @param tableType Types of tables to return, see
     * {@link java.sql.DatabaseMetaData#getTableTypes}
     */
    public ijResult showTables(String schema, String[] tableType) throws SQLException {
        ResultSet rs = null;
        try {
            haveConnection();

            DatabaseMetaData dbmd = theConnection.getMetaData();
            rs = dbmd.getTables(null, Utils.escape(schema),null, tableType);

            ColumnParameters[] columnParameters = new ColumnParameters[] {
                    new ColumnParameters( rs,"TABLE_SCHEM", 20),
                    new ColumnParameters( rs,"TABLE_NAME", 50),
                    new ColumnParameters( rs,"CONGLOM_ID", 10),
                    new ColumnParameters( rs,"REMARKS", 20)
            };
            return new ijResultSetResult(rs, columnParameters);
        } catch (SQLException e) {
            try {
                if(rs!=null)
                    rs.close();
            } catch (SQLException se) {
            }
            throw e;
        }
    }

    /**
     * Return a resultset of indexes for the given table or schema
     *
     * @param schema  schema to find indexes for
     * @param table the exact name of the table to find indexes for
     */
    private ResultSet getIndexInfoForTable(String schema, String table)
            throws SQLException {

        haveConnection();

        DatabaseMetaData dbmd = theConnection.getMetaData();
        return dbmd.getIndexInfo(null, Utils.escape(schema), Utils.escape(table), false, true);
    }

    /**
     * Used to show all indices.
     *
     * @param schema the schema indices are shown from.
     * @param table the table name to show indices for. If <code>null</code>,
     *      all indices of the schema are returned.
     */
    public ijResult showIndexes(String schema, String table)
            throws SQLException {

        ijResult result = null;

        int[] displayColumns = null;
        int[] columnWidths = null;

        try {
            ResultSet rs = getIndexInfoForTable(schema, table);
            ColumnParameters[] columnParameters = new ColumnParameters[] {
                    new ColumnParameters( rs, "TABLE_SCHEM", 20),
                    new ColumnParameters( rs, "TABLE_NAME", 50),
                    new ColumnParameters( rs, "INDEX_NAME", 50),
                    new ColumnParameters( rs, "COLUMN_NAME", 20),
                    new ColumnParameters( rs, "ORDINAL_POSITION", 8).maxWidth(16),
                    new ColumnParameters( rs, "NON_UNIQUE", 10).maxWidth(10),
                    new ColumnParameters( rs, "TYPE", 5).maxWidth(15),
                    new ColumnParameters( rs, "ASC_OR_DESC", 4).maxWidth( 11),
                    new ColumnParameters( rs, "CONGLOM_NO", 10).maxWidth(10)
            };
            if(schema!=null) {
                columnParameters = arraySubset(columnParameters, 1, columnParameters.length);
            }
            result = new ijResultSetResult(rs, columnParameters);
            return result;
        } catch (SQLException e) {
            throw e;
        }
    }


    /**
     Return a resultset of procedures from database metadata
     */
    public ijResult showProcedures(String schema) throws SQLException {
        ResultSet rs = null;
        try {
            haveConnection();

            DatabaseMetaData dbmd = theConnection.getMetaData();
            rs = dbmd.getProcedures(null, Utils.escape(schema),null);

            ColumnParameters[] columnParameters = new ColumnParameters[] {
                    new ColumnParameters( rs, "PROCEDURE_SCHEM", 20),
                    new ColumnParameters( rs, "PROCEDURE_NAME", 60),
                    new ColumnParameters( rs, "REMARKS", 100)
            };

            return new ijResultSetResult(rs, columnParameters);
        } catch (SQLException e) {
            try {
                if(rs!=null)
                    rs.close();
            } catch (SQLException se) {
            }
            throw e;
        }
    }

    /**
     * Verify that a procedure exists within a schema. Throws an exception
     * if procedure does not exist.
     * @param schema Schema for the table
     * @param proc  Name of procedure to check for existence of
     */
    public void verifyProcedureExists(String schema, String proc)
            throws SQLException {
        if(schema == null)
            return;

        ResultSet rs = null;
        try {
            DatabaseMetaData dbmd = theConnection.getMetaData();
            rs = dbmd.getProcedures(null, Utils.escape(schema), Utils.escape(proc));
            if(!rs.next())
                throw ijException.noSuchProcedure(proc);
        } finally {
            try {
                if(rs!=null)
                    rs.close();
            } catch (SQLException e) {
            }
        }
    }
    /**
     * Return a resultset of procedure columns from database metadata
     */
    public ijResult showProcedureColumns(String schema, String proc) throws SQLException {
        ResultSet rs = null;
        try {
            haveConnection();
            verifyProcedureExists(schema,proc);

            DatabaseMetaData dbmd = theConnection.getMetaData();
            rs = dbmd.getProcedureColumns(null,Utils.escape(schema), Utils.escape(proc),null);

            // Small subset of the result set fields available
            ColumnParameters[] columnParameters = new ColumnParameters[] {
                    new ColumnParameters( rs, "COLUMN_NAME", 32),
                    new ColumnParameters( rs, "TYPE_NAME", 32).maxWidth(16),
                    new ColumnParameters( rs, "ORDINAL_POSITION", 16).maxWidth(8)
            };

            return new ijResultSetResult(rs, columnParameters);
        } catch (SQLException e) {
            try {
                if(rs!=null)
                    rs.close();
            } catch (SQLException se) {
            }
            throw e;
        }
    }

    /**
     * Return a resultset of primary keys from database metadata
     */
    public ijResult showPrimaryKeys(String schema, String table) throws SQLException {
        ResultSet rs = null;
        try {
            haveConnection();

            DatabaseMetaData dbmd = theConnection.getMetaData();
            if(hasServerLikeFix())
                rs = dbmd.getPrimaryKeys(null,Utils.escape(schema),Utils.escape(table));
            else
                rs = dbmd.getPrimaryKeys(null,schema,table);
            ColumnParameters[] columnParameters = new ColumnParameters[] {
                    new ColumnParameters( rs, "TABLE_NAME", 30),
                    new ColumnParameters( rs, "COLUMN_NAME", 30),
                    new ColumnParameters( rs, "KEY_SEQ", 10),
                    new ColumnParameters( rs, "PK_NAME", 30)
            };
            return new ijResultSetResult(rs, columnParameters);
        } catch (SQLException e) {
            try {
                if(rs!=null)
                    rs.close();
            } catch (SQLException se) {
            }
            throw e;
        }
    }

    /**
     * Return a resultset of functions from database metadata.
     *
     * JDBC4.0 has a method in DatabaseMetaData called getFunctions().
     * Since this method is implemented in Derby's JDBC3.0 driver
     * we can use it. But only through Java reflection.
     */
    public ijResult showFunctions(String schema) throws SQLException {
        ResultSet rs = null;

        try {
            haveConnection();

            DatabaseMetaData dbmd = theConnection.getMetaData();
            Method getFunctions;
            try {
                getFunctions = dbmd.getClass().getMethod("getFunctions",
                        new Class[] { String.class,
                                String.class,
                                String.class});
                rs = (ResultSet)getFunctions.invoke(dbmd, new Object[] { null, Utils.escape(schema), null});
            } catch(NoSuchMethodException nsme) {
                throw ijException.notAvailableForDriver(dbmd.getDriverName());
            } catch(IllegalAccessException iae) {
                throw ijException.notAvailableForDriver(dbmd.getDriverName());
            } catch(AbstractMethodError ame) {
                // According to http://bugs.sun.com/view_bug.do?bug_id=6531596
                // invoke() may throw AbstractMethodError instead of
                // InvocationTargetException on some JREs
                throw ijException.notAvailableForDriver(dbmd.getDriverName());
            } catch(InvocationTargetException ite) {
                Throwable cause = ite.getCause();
                // 'cause' *must* be an SQLException if the method is
                // *actually* called. But may be AbstractMethodError in some
                // cases, if the driver implements an older version of the
                // JDBC spec (pre-JDBC 4.0). See issue DERBY-3809.
                if (cause instanceof SQLException)
                    throw (SQLException)cause;

                // else
                throw ijException.notAvailableForDriver(dbmd.getDriverName());
            }

            ColumnParameters[] columnParameters = new ColumnParameters[] {
                    new ColumnParameters( rs, "FUNCTION_SCHEM", 14),
                    new ColumnParameters( rs, "FUNCTION_NAME", 35),
                    new ColumnParameters( rs, "REMARKS", 80)
            };

            return new ijResultSetResult(rs, columnParameters);
        } catch (SQLException e) {
            try {
                if(rs!=null)
                    rs.close();
            } catch (SQLException se) {
            }
            throw e;
        }
    }

    /**
     * Return a resultset of schemas from database metadata
     */
    public ijResult showSchemas() throws SQLException {
        ResultSet rs = null;
        try {
            haveConnection();

            DatabaseMetaData dbmd = theConnection.getMetaData();
            rs = dbmd.getSchemas();

            ColumnParameters[] columnParameters = new ColumnParameters[] {
                    new ColumnParameters( rs, "TABLE_SCHEM", 30)
            };

            return new ijResultSetResult(rs, columnParameters);
        } catch (SQLException e) {
            try {
                if(rs!=null)
                    rs.close();
            } catch (SQLException se) {
            }
            throw e;
        }
    }

    /**
     * Return a resultset of roles. No database metadata
     * available, so select from SYS.SYSROLES directly. This has
     * the side effect of starting a transaction if one is not
     * already active, so we should perhaps give warning when not
     * in autocommit mode.
     */
    public ijResult showRoles() throws SQLException {
        ResultSet rs = null;
        try {
            haveConnection();

            rs = theConnection.createStatement().executeQuery
                    ("SELECT ROLEID FROM SYS.SYSROLES WHERE ISDEF='Y' " +
                            "ORDER BY ROLEID ASC");

            ColumnParameters[] columnParameters = new ColumnParameters[] {
                    new ColumnParameters( rs, "ROLEID", 30)
            };

            return new ijResultSetResult(rs, columnParameters);
        } catch (SQLException e) {
            try {
                if(rs!=null)
                    rs.close();
            } catch (SQLException se) {
            }
            throw e;
        }
    }

    /**
     * Return a resultset of settable roles, sorted on ROLEID.  This has the
     * side effect of starting a transaction if one is not already active, so
     * we should perhaps give warning when not in autocommit mode.
     */
    public ijResult showSettableRoles() throws SQLException {
        ResultSet rs = null;
        final String query  =
                // Ordinary user is restricted to roles explicitly granted:
                "select distinct * from (" +
                        "  select roleid from sys.sysroles s" +
                        "    where s.grantee = current_user or s.grantee = 'PUBLIC'" +
                        "  union" +
                        // Data base owner can set all roles:
                        "  select roleid from sys.sysroles s" +
                        "    where s.isdef='Y' and current_user in" +
                        "        (select authorizationid from sys.sysschemas" +
                        "             where schemaname = 'SYS')) t " +
                        "order by roleid";

        try {
            haveConnection();

            rs = theConnection.createStatement().executeQuery(query);

            ColumnParameters[] columnParameters = new ColumnParameters[] {
                    new ColumnParameters( rs, "ROLEID", 30)
            };

            return new ijResultSetResult(rs, columnParameters);
        } catch (SQLException e) {
            try {
                if(rs!=null)
                    rs.close();
            } catch (SQLException se) {
            }
            throw e;
        }
    }
    /**
     * Outputs the DDL of given table.
     */
    @SuppressFBWarnings("SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE") // intentional
    public ijResult showCreateTable(String schema, String table) throws SQLException {
        ResultSet rs = null;
        try {
            haveConnection();
            Statement stmt = theConnection.createStatement();
            rs = stmt.executeQuery("CALL SYSCS_UTIL.SHOW_CREATE_TABLE(\'" +
                    schema + "\'," + "\'" + table + "\')" );
            int[] displayColumns = new int[] { 1 };
            int[] columnWidths = new int[] { 0 };
            return new ijResultSetResult(rs, displayColumns, columnWidths);
        } catch (SQLException e) {
            try {
                if(rs!=null)
                    rs.close();
            } catch (SQLException se) {
            }
            throw e;
        }
    }
    /**
     * Same as SYSCS_UTIL.SYSCS_GET_VERSION_INFO()
     * and SYSCS_UTIL.SYSCS_GET_VERSION_INFO_LOCAL()
     */
    public ijResult showVersionLocal(boolean local) throws SQLException {
        ResultSet rs = null;
        String procedurePostfix = local ? "_LOCAL()" : "()";
        try {
            haveConnection();
            Statement stmt = theConnection.createStatement();
            rs = stmt.executeQuery("CALL SYSCS_UTIL.SYSCS_GET_VERSION_INFO" + procedurePostfix );
            int[] displayColumns = new int[] { 1,2,3,4,5 };
            int[] columnWidths = new int[] { 0,0,0,0,0};
            return new ijResultSetResult(rs, displayColumns, columnWidths);
        } catch (SQLException e) {
            try {
                if(rs!=null)
                    rs.close();
            } catch (SQLException se) {
            }
            throw e;
        }
    }

    /**
     * Verify that a table exists within a schema. Throws an exception
     * if table does not exist.
     *
     * @param schema Schema for the table
     * @param table  Name of table to check for existence of
     */
    public void verifyTableExists(String schema, String table)
            throws SQLException {
        if(schema == null)
            return;

        ResultSet rs = null;
        try {
            DatabaseMetaData dbmd = theConnection.getMetaData();

            rs = dbmd.getSchemas(null,schema);
            if (!rs.next())
                throw ijException.noSuchSchema(schema);
            rs.close();

            rs = dbmd.getTables(null,schema,table,null);
            if(!rs.next())
                throw ijException.noSuchTable(table);
        } finally {
            try {
                if(rs!=null)
                    rs.close();
            } catch (SQLException e) {
            }
        }
    }
    /**
     * Outputs the names of all fields of given table. Outputs field
     * names and data type.
     */
    public ijResult describeTable(String schema, String table) throws SQLException {
        ResultSet rs = null;
        try {
            haveConnection();
            verifyTableExists(schema,table);

            DatabaseMetaData dbmd = theConnection.getMetaData();

            //Check if it's a synonym table
            String getSynonymAliasInfo = String.format(
                    "SELECT BASETABLE FROM SYSVW.SYSALIASTOTABLEVIEW V WHERE (V.SCHEMANAME LIKE ? ESCAPE '%s') AND (V.ALIAS LIKE ? ESCAPE '%s')",
                    Utils.defaultEscapeCharacter, Utils.defaultEscapeCharacter);

            PreparedStatement s = theConnection.prepareStatement(getSynonymAliasInfo);
            s.setString(1, Utils.escape(schema));
            s.setString(2, Utils.escape(table));
            rs = s.executeQuery();

            if (rs.next()){
                String[] fullName = rs.getString(1).split("\\.");
                if (fullName.length == 2) {
                    schema = fullName[0].substring(1,fullName[0].length()-1);
                    table = fullName[1].substring(1,fullName[1].length()-1);
                }
            }

            if(hasServerLikeFix())
                rs = dbmd.getColumns(null,Utils.escape(schema),Utils.escape(table),null);
            else
                rs = dbmd.getColumns(null,schema,table,null);

            ColumnParameters[] columnParameters = new ColumnParameters[] {
                    new ColumnParameters( rs, "TABLE_SCHEM", 20),
                    new ColumnParameters( rs, "TABLE_NAME", 20),
                    new ColumnParameters( rs, "COLUMN_NAME", 40),
                    new ColumnParameters( rs, "TYPE_NAME", 9).maxWidth(15),
                    new ColumnParameters( rs, "DECIMAL_DIGITS", 4).maxWidth(14),
                    new ColumnParameters( rs, "NUM_PREC_RADIX", 4).maxWidth(14),
                    new ColumnParameters( rs, "COLUMN_SIZE", 10).maxWidth(11),
                    new ColumnParameters( rs, "COLUMN_DEF", 10).maxWidth(10),
                    new ColumnParameters( rs, "CHAR_OCTET_LENGTH", 10).maxWidth(17),
                    new ColumnParameters( rs, "IS_NULLABLE", 6).maxWidth(11)
            };
            // If schema is specified (if util.getSelectedSchema in
            // DescTableStatement() returns correct value), then we
            // don't need to output schema and table names.
            if(schema!=null && table != null) {
                columnParameters = arraySubset(columnParameters, 2, columnParameters.length);
            }
            return new ijResultSetResult(rs, columnParameters);
        } catch (SQLException e) {
            try {
                if(rs!=null)
                    rs.close();
            } catch (SQLException se) {
            }
            throw e;
        }
    }

    private String[] sortConnectionNames()
    {
        int size = 100;
        int count = 0;
        String[] array = new String[size];
        String key;

        Hashtable ss = currentConnEnv.getSessions();
        // Calculate the number of connections in the sessions list and
        // build an array of all the connection names.
        for (Enumeration connectionNames = ss.keys(); connectionNames.hasMoreElements();) {
            if (count == size) {
                // need to expand the array
                size = size*2;
                String[] expandedArray = new String[size];
                System.arraycopy(array, 0, expandedArray, 0, count);
                array = expandedArray;
            }
            key = (String)connectionNames.nextElement();
            array[ count++ ] = key;
        }

        java.util.Arrays.sort(array, 0, count);

        return array;
    }

    /**
     * This is used at the ij startup time to see if there are already some
     * connections made and if so, show connections made so far.
     * Following also gets executed when user types show connections command
     * in ij. In the former case, ignore0Rows is set whereas in the later cas
     * it's set to false. The reason for this is, at ij startup time, if there
     * are no connections made so far, we don't want to show anything. Only if
     * there are connections made, we show the connections. Whereas in show
     * connection command case, we want to show the connection status either way
     * ie if there are no connections, we say no connections. Otherwise we list
     * all the connections made so far.
     */
    public ijResult showConnectionsMethod(boolean ignore0Rows) throws SQLException {
        Hashtable ss = currentConnEnv.getSessions();
        Vector v = new Vector();
        SQLWarning w = null;
        if (ss == null || ss.size() == 0) {
            if (!ignore0Rows)
                v.addElement(LocalizedResource.getMessage("IJ_NoConneAvail"));
        }
        else {
            boolean haveCurrent=false;
            int count = 0;
            for (Enumeration connectionNames = ss.keys(); connectionNames.hasMoreElements();
                 connectionNames.nextElement())
                count++;
            String[] array = sortConnectionNames();
            for ( int ictr = 0; ictr < count; ictr++ ) {
                String connectionName = array[ ictr ];
                Session s = (Session)ss.get(connectionName);
                if (s.getConnection().isClosed()) {
                    if (currentConnEnv.getSession() != null &&
                            connectionName.equals(currentConnEnv.getSession().getName())) {
                        currentConnEnv.removeCurrentSession();
                        theConnection = null;
                    }
                    else
                        currentConnEnv.removeSession(connectionName);
                }
                else {
                    StringBuilder row = new StringBuilder();
                    row.append(connectionName);
                    if (currentConnEnv.getSession() != null &&
                            connectionName.equals(currentConnEnv.getSession().getName())) {
                        row.append('*');
                        haveCurrent=true;
                    }

                    //If ij.dataSource property is set, show only connection names.
                    //In this case, URL is not used to get connection, so do not append URL
                    String dsName = util.getSystemProperty("ij.dataSource");
                    if(dsName == null){
                        row.append(" - 	");
                        String url = s.getConnection().getMetaData().getURL();
                        url = url.replace(":derby:", ":splice:");
                        row.append(url);
                    }
                    // save the warnings from these connections
                    w = appendWarnings(w,s.getConnection().getWarnings());
                    s.getConnection().clearWarnings();
                    v.addElement(row.toString());
                }
            }
            if (haveCurrent)
                v.addElement(LocalizedResource.getMessage("IJ_CurreConne"));
            else
                v.addElement(LocalizedResource.getMessage("IJ_NoCurreConne"));
        }
        return new ijVectorResult(v,w);
    }
    /**
     Add the warnings of wTail to the end of those of wHead.
     */
    public static SQLWarning appendWarnings(SQLWarning wHead, SQLWarning wTail) {
        if (wHead == null) return wTail;

        if (wHead.getNextException() == null) {
            wHead.setNextException(wTail);
        } else {
            appendWarnings(wHead.getNextWarning(), wTail);
        }
        return wHead;
    }

}
