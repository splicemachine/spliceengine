package com.splicemachine.db.impl.tools.ij;

import com.splicemachine.db.iapi.tools.i18n.LocalizedResource;
import com.splicemachine.db.tools.JDBCDisplayUtil;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.*;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Locale;
import java.util.Properties;

public class ijImpl extends ijCommands {
    static final String PROTOCOL_PROPERTY = "ij.protocol";
    static final String URLCHECK_PROPERTY = "ij.URLCheck";
    static final String USER_PROPERTY = "ij.user";
    static final String PASSWORD_PROPERTY = "ij.password";
    static final String FRAMEWORK_PROPERTY = "framework";

    static final String DOUBLEQUOTES = "\"\"";

    boolean elapsedTime = true;
    boolean progressBar = false;

    String urlCheck = null;

    xaAbstractHelper xahelper = null;
    boolean exit = false;

    Hashtable ignoreErrors = null;
    String protocol = null; // the (single) unnamed protocol
    Hashtable namedProtocols;


    ijImpl() {}

    /**
     Initialize this parser from the environment
     (system properties). Used when ij is being run
     as a command line program.
     */
    public void initFromEnvironment() {

        // load all protocols specified via properties
        //
        Properties p = (Properties) AccessController.doPrivileged(new PrivilegedAction() {
            public Object run() {
                return System.getProperties();
            }
        });
        urlCheck = p.getProperty(URLCHECK_PROPERTY);
        protocol = p.getProperty(PROTOCOL_PROPERTY);
        String framework_property = p.getProperty(FRAMEWORK_PROPERTY);

        if (JDBC20X() && JTA() && JNDI())
        {
            try {
                xahelper = (xaAbstractHelper) Class.forName("com.splicemachine.dbpl.tools.ij.xaHelper").newInstance();
                xahelper.setFramework(framework_property);
            } catch (Exception e) {
            }

        }


        namedProtocols = new Hashtable();
        String prefix = PROTOCOL_PROPERTY + ".";
        for (Enumeration e = p.propertyNames(); e.hasMoreElements(); )
        {
            String key = (String)e.nextElement();
            if (key.startsWith(prefix)) {
                String name = key.substring(prefix.length());
                installProtocol(name.toUpperCase(Locale.ENGLISH), p.getProperty(key));
            }
        }
    }
    /**
     * Return whether or not JDBC 2.0 (and greater) extension classes can be loaded
     *
     * @return true if JDBC 2.0 (and greater) extension classes can be loaded
     */
    private static boolean JDBC20X()
    {
        try
        {
            Class.forName("javax.sql.DataSource");
            Class.forName("javax.sql.ConnectionPoolDataSource");
            Class.forName("javax.sql.PooledConnection");
            Class.forName("javax.sql.XAConnection");
            Class.forName("javax.sql.XADataSource");
        }
        catch(ClassNotFoundException cnfe)
        {
            return false;
        }
        return true;
    }
    /**
     * Return whether or not JTA classes can be loaded
     *
     * @return true if JTA classes can be loaded
     */
    private static boolean JTA()
    {
        try
        {
            Class.forName("javax.transaction.xa.Xid");
            Class.forName("javax.transaction.xa.XAResource");
            Class.forName("javax.transaction.xa.XAException");
        }
        catch(ClassNotFoundException cnfe)
        {
            return false;
        }
        return true;
    }

    /**
     * Return whether or not JNDI extension classes can be loaded
     *
     * @return true if JNDI extension classes can be loaded
     */
    private static boolean JNDI()
    {
        try
        {
            Class.forName("javax.naming.spi.Resolver");
            Class.forName("javax.naming.Referenceable");
            Class.forName("javax.naming.directory.Attribute");
        }
        catch(ClassNotFoundException cnfe)
        {
            return false;
        }
        return true;
    }

    /**
     * Get the "elapsedTime state".
     */
    boolean getElapsedTimeState()
    {
        return elapsedTime;
    }

    /**
     this removes the outside quotes from the string.
     it will also swizzle the special characters
     into their actual characters, like '' for ', etc.
     */
    String stringValue(String s) {
        String result = s.substring(1,s.length()-1);
        char quotes = '\'';
        int index;

        /* Find the first occurrence of adjacent quotes. */
        index = result.indexOf(quotes);

        /* Replace each occurrence with a single quote and begin the
         * search for the next occurrence from where we left off.
         */
        while (index != -1)
        {
            result = result.substring(0, index + 1) + result.substring(index + 2);

            index = result.indexOf(quotes, index + 1);
        }

        return result;
    }

    void installProtocol(String name, String value) {
        try {
            // `value' is a JDBC protocol;
            // we load the "driver" in the prototypical
            // manner, it will register itself with
            // the DriverManager.
            util.loadDriverIfKnown(value);
        } catch (ClassNotFoundException e) {
            throw ijException.classNotFoundForProtocol(value);
        } catch (IllegalArgumentException e) {
            throw ijException.classNotFoundForProtocol(value);
        } catch (IllegalAccessException e) {
            throw ijException.classNotFoundForProtocol(value);
        } catch (InstantiationException e) {
            throw ijException.classNotFoundForProtocol(value);
        }
        if (name == null)
            protocol = value;
        else
            namedProtocols.put(name, value);
    }

// FIXME: caller has to deal with ignoreErrors and handleSQLException behavior

    /**
     Find a session by its name. Throws an exception if the session does
     not exists.
     */
    Session findSession(String name) {
        Session session = currentConnEnv.getSession(name);

        if (session == null)
            throw ijException.noSuchConnection(name);

        return session;
    }

    /**
     Find a prepared statement. Throws an exception if the session does
     not exists or the prepared statement can't be found.
     */
    PreparedStatement findPreparedStatement(QualifiedIdentifier qi) {
        Session session = findSession(qi.getSessionName());
        PreparedStatement ps = session.getPreparedStatement(qi.getLocalName());

        JDBCDisplayUtil.checkNotNull(ps, "prepared statement " + qi);

        return ps;
    }

    /**
     Find a cursor. Throws an exception if the session does not exits or
     it deosn't have the correspondig cursor.
     */
    ResultSet findCursor(QualifiedIdentifier qi) {
        Session session = findSession(qi.getSessionName());
        ResultSet c = session.getCursor(qi.getLocalName());

        JDBCDisplayUtil.checkNotNull(c, "cursor " + qi);

        return c;
    }

    /**
     We do not reuse statement objects at all, because
     some systems require you to close the object to release
     resources (JBMS), while others will not let you reuse
     the statement object once it is closed (WebLogic).

     If you want to reuse statement objects, you need to
     use the ij PREPARE and EXECUTE statements.

     @param stmt the statement

     **/
    ijResult executeImmediate(String stmt) throws SQLException {
        Statement aStatement = null;
        try {
            long beginTime = 0;
            long endTime = 0;
            boolean cleanUpStmt = false;

            haveConnection();
            aStatement = theConnection.createStatement();

            // for JCC - remove comments at the beginning of the statement
            // and trim; do the same for Derby Clients that have versions
            // earlier than 10.2.
            if (currentConnEnv != null) {
                boolean trimForDNC = currentConnEnv.getSession().getIsDNC();
                if (trimForDNC) {
                    // we're using the Derby Client, but we only want to trim
                    // if the version is earlier than 10.2.
                    DatabaseMetaData dbmd = theConnection.getMetaData();
                    int majorVersion = dbmd.getDriverMajorVersion();
                    if ((majorVersion > 10) || ((majorVersion == 10) &&
                            (dbmd.getDriverMinorVersion() > 1)))
                    { // 10.2 or later, so don't trim/remove comments.
                        trimForDNC = false;
                    }
                }
                if (currentConnEnv.getSession().getIsJCC() || trimForDNC) {
                    // remove comments and trim.
                    int nextline;
                    while(stmt.startsWith("--"))
                    {
                        nextline = stmt.indexOf('\n')+1;
                        stmt = stmt.substring(nextline);
                    }
                    stmt = stmt.trim();
                }
            }

            aStatement.execute(stmt);

            // FIXME: display results. return start time.
            return new ijStatementResult(aStatement,true);

        } catch (SQLException e) {
            try {
                if (aStatement!=null)  // free the resource
                    aStatement.close();
            } catch (SQLException se) {
            }
            throw e;
        }
    }

    ijResult quit() throws SQLException {
        exit = true;
        if (getExpect()) { // report stats
            // FIXME: replace with MVC...
            // FIXME: this is a kludgy way to quiet /0 and make 0/0=1...
            int numExpectOr1 = (numExpect==0?1:numExpect);
            int numPassOr1 = (numPass==numExpect && numPass==0)?1:numPass;
            int numFailOr1 = (numFail==numExpect && numFail==0)?1:numFail;
            int numUnxOr1 = (numUnx==numExpect && numUnx==0)?1:numUnx;

            LocalizedResource.OutputWriter().println(LocalizedResource.getMessage("IJ_TestsRun0Pass12Fail34",
                    new Object[]{
                            LocalizedResource.getNumber(numExpect), LocalizedResource.getNumber(100*(numPassOr1/numExpectOr1)),
                            LocalizedResource.getNumber(100*(numFailOr1/numExpectOr1))}));
            if (numUnx > 0) {
                LocalizedResource.OutputWriter().println();
                LocalizedResource.OutputWriter().println(LocalizedResource.getMessage("IJ_UnexpResulUnx01",
                        LocalizedResource.getNumber(numUnx), LocalizedResource.getNumber(100*(numUnxOr1/numExpectOr1))));
            }
        }
        currentConnEnv.removeAllSessions();
        theConnection = null;
        return null;
    }

    /**
     Async execution wants to return results off-cycle.
     We want to control their output, and so will hold it
     up until it is requested with a WAIT FOR asyncName
     statement.  WAIT FOR will return the results of
     the async statement once they are ready.  Note that using
     a select only waits for the execute to complete; the
     logic to step through the result set is in the caller.
     **/
    ijResult executeAsync(String stmt, QualifiedIdentifier qi) {
        Session sn = findSession(qi.getSessionName());
        AsyncStatement as = new AsyncStatement(sn.getConnection(), stmt);

        sn.addAsyncStatement(qi.getLocalName(),as);

        as.start();

        return null;
    }



    void setConnection(ConnectionEnv connEnv, boolean multipleEnvironments) {
        Connection conn = connEnv.getConnection();

        if (connEnv != currentConnEnv) // single connenv is common case
            currentConnEnv = connEnv;

        if (theConnection == conn) return; // not changed.

        if ((theConnection == null) || multipleEnvironments) {
            // must have switched env's (could check)
            theConnection = conn;
        } else {
            throw ijException.needToDisconnect();
        }
    }

    /**
     Note the Expect Result in the output and in the stats.
     FIXME
     */
    int numExpect, numPass, numFail, numUnx;
    void noteExpect(boolean actual, boolean want) {
        numExpect++;
        if (actual) numPass++;
        else numFail++;

        LocalizedResource.OutputWriter().print(LocalizedResource.getMessage(actual?"IJ_Pass":"IJ_Fail"));
        if (actual != want) {
            numUnx++;
            LocalizedResource.OutputWriter().println(LocalizedResource.getMessage("IJ_Unx"));
        }
        else LocalizedResource.OutputWriter().println();
    }

    boolean getExpect() {
        String s = util.getSystemProperty("ij.expect");
        return Boolean.valueOf(s).booleanValue();
    }

    ijResult addSession(Connection newConnection, String name) throws SQLException
    {
        if (currentConnEnv.haveSession(name)) {
            throw ijException.alreadyHaveConnectionNamed(name);
        }

        currentConnEnv.addSession( newConnection, name );
        return new ijConnectionResult( newConnection );
    }

    private Object makeXid(int xid)
    {
        return null;
    }

    void set_xplain_trace(boolean enable) throws SQLException{

        haveConnection();
        Statement aStatement = theConnection.createStatement();

        if (enable) {
            aStatement.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)");
            aStatement.execute("call SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(1)");
        }
        else {
            aStatement.execute("call SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(0)");
            aStatement.execute("call SYSCS_UTIL.SYSCS_SET_STATISTICS_TIMING(0)");
        }

    }

    /*
     * Compress 2 adjacent (single or double) quotes into a single (s or d)
     * quote when found in the middle of a String.
     * This function comes from com.splicemachine.db.iapi.util.StringUtil
     * todo: merge with iapi.util.StringUtil
     */
    private static String compressQuotes(String source,String quotes)
    {

        String result=source;
        int index;

        /* Find the first occurrence of adjacent quotes. */
        index=result.indexOf(quotes);

        /* Replace each occurrence with a single quote and begin the
         * search for the next occurrence from where we left off.
         */
        while(index!=-1){
            result=result.substring(0,index+1)+ result.substring(index+2);
            index=result.indexOf(quotes,index+1);
        }

        return result;
    }

    static String normalizeDelimitedID(String str)
    {
        str = compressQuotes(str, DOUBLEQUOTES);
        return str;
    }

    String castIdentifier(String identifier) throws SQLException
    {
        haveConnection();
        DatabaseMetaData dbmd = theConnection.getMetaData();

        if (dbmd.storesLowerCaseIdentifiers())
            return identifier.toLowerCase(Locale.ENGLISH);
        else if (dbmd.storesUpperCaseIdentifiers())
            return identifier.toUpperCase(Locale.ENGLISH);

        return identifier;
    }

    public ijResult connect(boolean simplifiedPath, String protocolIn,
                            String userS, String passwordS, String connectionStr, String name) throws SQLException {
        String sVal;
        Properties connInfo = new Properties();

        //If ij.dataSource property is set,use DataSource to get the connection
        String dsName = util.getSystemProperty("ij.dataSource");
        if (dsName != null){
            //Check that t.image does not start with jdbc:
            //If it starts with jdbc:, do not use DataSource to get connection
            sVal = connectionStr;
            if(!sVal.startsWith("jdbc:") ){
                theConnection = util.getDataSourceConnection(dsName,userS,passwordS,sVal,false);
                return addSession( theConnection, name );
            }
        }

        if (simplifiedPath)
            // url for the database W/O 'jdbc:protocol:', i.e. just a dbname
            // For example,
            //  CONNECT TO 'test'
            // is equivalent to
            //   CONNECT TO 'jdbc:splice:test'
            sVal = "jdbc:splice:" + connectionStr;
        else
            sVal = connectionStr;

        // add named protocol if it was specified
        if (protocolIn != null) {
            String protocol = (String)namedProtocols.get(protocolIn);
            if (protocol == null) { throw ijException.noSuchProtocol(protocolIn); }
            sVal = protocol + sVal;
        }

        // add protocol if no driver matches url
        boolean noDriver = false;
        // if we have a full URL, make sure it's loaded first
        try {
            if (sVal.startsWith("jdbc:"))
                util.loadDriverIfKnown(sVal);
        } catch (Exception e) {
            // want to continue with the attempt
        }
        // By default perform extra checking on the URL attributes.
        // This checking does not change the processing.
        if (urlCheck == null || Boolean.valueOf(urlCheck).booleanValue()) {
            URLCheck aCheck = new URLCheck(sVal);
        }
        if (!sVal.startsWith("jdbc:") && (protocolIn == null) && (protocol != null)) {
            sVal = protocol + sVal;
        }


        // If no ATTRIBUTES on the connection get them from the
        // defaults
        connInfo = util.updateConnInfo(userS,passwordS, connInfo);


        theConnection = DriverManager.getConnection(sVal,connInfo);
        theConnection2 = DriverManager.getConnection(sVal,connInfo);
        return addSession( theConnection, name );
    }


}
