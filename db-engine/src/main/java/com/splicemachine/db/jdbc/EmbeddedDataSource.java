/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.jdbc;

import com.splicemachine.db.iapi.reference.Attribute;

import java.sql.*;

import java.io.PrintWriter;
import java.util.Objects;
import java.util.Properties;
import java.util.logging.Logger;

/* -- New jdbc 20 extension types --- */


import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.impl.jdbc.Util;

/** 


    EmbeddedDataSource is Derby's DataSource implementation for JDBC3.0.


    <P>A DataSource  is a factory for Connection objects. An object that
    implements the DataSource interface will typically be registered with a
    JNDI service provider.
    <P>
    EmbeddedDataSource automatically supports the correct JDBC specification version
    for the Java Virtual Machine's environment.
    <UL>
    <LI> JDBC 3.0 - Java 2 - JDK 1.4, J2SE 5.0
    </UL>

    <P>The following is a list of properties that can be set on a Derby
    DataSource object:
    <P><B>Standard DataSource properties</B> (from JDBC 3.0 specification).

    <UL><LI><B><code>databaseName</code></B> (String): <I>Mandatory</I>
    <BR>This property must be set and it
    identifies which database to access.  If a database named wombat located at
    g:/db/wombat is to be accessed, then one should call
    <code>setDatabaseName("g:/db/wombat")</code> on the data source object.</LI>

    <LI><B><code>dataSourceName</code></B> (String): <I>Optional</I>
    <BR> Name for DataSource.  Not used by the data source object.  Used for
    informational purpose only.</LI>

    <LI><B><code>description</code></B> (String): <I>Optional</I>
    <BR>Description of the data source.  Not
    used by the data source object.  Used for informational purpose only.</LI>

    <LI><B><code>password</code></B> (String): <I>Optional</I>
    <BR>Database password for the no argument <code>DataSource.getConnection()</code>,
    <code>ConnectionPoolDataSource.getPooledConnection()</code>
    and <code>XADataSource.getXAConnection()</code> methods.

    <LI><B><code>user</code></B> (String): <I>Optional</I>
    <BR>Database user for the no argument <code>DataSource.getConnection()</code>,
    <code>ConnectionPoolDataSource.getPooledConnection()</code>
    and <code>XADataSource.getXAConnection()</code> methods.
    </UL>

    <BR><B>Derby specific DataSource properties.</B>

  <UL>

  <LI><B><code>attributesAsPassword</code></B> (Boolean): <I>Optional</I>
    <BR>If true, treat the password value in a
    <code>DataSource.getConnection(String user, String password)</code>,
    <code>ConnectionPoolDataSource.getPooledConnection(String user, String password)</code>
    or <code>XADataSource.getXAConnection(String user, String password)</code> as a set
    of connection attributes. The format of the attributes is the same as the format
    of the attributes in the property connectionAttributes. If false the password value
    is treated normally as the password for the given user.
    Setting this property to true allows a connection request from an application to
    provide more authentication information that just a password, for example the request
    can include the user's password and an encrypted database's boot password.</LI>

  <LI><B><code>connectionAttributes</code></B> (String): <I>Optional</I>
  <BR>Defines a set of Derby connection attributes for use in all connection requests.
  The format of the String matches the format of the connection attributes in a Derby JDBC URL.
  That is a list of attributes in the form <code><I>attribute</I>=<I>value</I></code>, each separated by semi-colon (';').
  E.g. <code>setConnectionAttributes("bootPassword=erd3234dggd3kazkj3000");</code>.
  <BR>The database name must be set by the DataSource property <code>databaseName</code> and not by setting the <code>databaseName</code>
  connection attribute in the <code>connectionAttributes</code> property.
    <BR>
   Any attributes that can be set using a property of this DataSource implementation
   (e.g user, password) should not be set in connectionAttributes. Conflicting
   settings in connectionAttributes and properties of the DataSource will lead to
   unexpected behaviour. 
  <BR>Please see the Derby documentation for a complete list of connection attributes. </LI>

  <LI><B><code>createDatabase</code></B> (String): <I>Optional</I>
    <BR>If set to the string "create", this will
    cause a new database of <code>databaseName</code> if that database does not already
    exist.  The database is created when a connection object is obtained from
    the data source. </LI>

    <LI><B><code>shutdownDatabase</code></B> (String): <I>Optional</I>
    <BR>If set to the string "shutdown",
    this will cause the database to shutdown when a java.sql.Connection object
    is obtained from the data source.  E.g., If the data source is an
    XADataSource, a getXAConnection().getConnection() is necessary to cause the
    database to shutdown.

    </UL>

    <P><B>Examples.</B>

    <P>This is an example of setting a property directly using Derby's
    EmbeddedDataSource object.  This code is typically written by a system integrator :
    <PRE>
    *
    * import com.splicemachine.db.jdbc.*;
    *
    * // dbname is the database name
    * // if create is true, create the database if necessary
    * javax.sql.DataSource makeDataSource (String dbname, boolean create)
    *    throws Throwable
    * {
    *    EmbeddedDataSource ds = new EmbeddedDataSource();
    *    ds.setDatabaseName(dbname);
    *
    *    if (create)
    *        ds.setCreateDatabase("create");
    *   
    *    return ds;
    * }
    </PRE>

    <P>Example of setting properties thru reflection.  This code is typically
    generated by tools or written by a system integrator: <PRE>
    *
    * javax.sql.DataSource makeDataSource(String dbname)
    *    throws Throwable
    * {
    *    Class[] parameter = new Class[1];
    *    parameter[0] = dbname.getClass();
    *    DataSource ds =  new EmbeddedDataSource();
    *    Class cl = ds.getClass();
    *
    *    Method setName = cl.getMethod("setDatabaseName", parameter);
    *    Object[] arg = new Object[1];
    *    arg[0] = dbname;
    *    setName.invoke(ds, arg);
    *
    *    return ds;
    * }
    </PRE>

    <P>Example on how to register a data source object with a JNDI naming
    service.
    <PRE>
    * DataSource ds = makeDataSource("mydb");
    * Context ctx = new InitialContext();
    * ctx.bind("jdbc/MyDB", ds);
    </PRE>

    <P>Example on how to retrieve a data source object from a JNDI naming
    service.
    <PRE>
    * Context ctx = new InitialContext();
    * DataSource ds = (DataSource)ctx.lookup("jdbc/MyDB");
    </PRE>

*/
public class EmbeddedDataSource extends ReferenceableDataSource implements
                javax.sql.DataSource
{
    private static final long serialVersionUID = -2912818644839926306L;

    /** instance variables that will be serialized */

    /**
     * Set to "create" if the database should be created.
     * @serial
     */
    private String createDatabase;

    /**
     * Set to "shutdown" if the database should be shutdown.
     * @serial
     */
    private String shutdownDatabase;

    /**
     * Derby specific connection attributes.
     * @serial
     */
    private String connectionAttributes;

    /**
        Set password to be a set of connection attributes.
    */
    private boolean attributesAsPassword;

    /** instance variables that will not be serialized */
    transient private PrintWriter printer;
    transient private int loginTimeout;

    // Unlike a DataSource, LocalDriver is shared by all
    // Derby databases in the same jvm.
    transient InternalDriver driver;

    transient private String jdbcurl;

    /**
        No-arg constructor.
     */
    public EmbeddedDataSource() {
        // needed by Object Factory

        // don't put anything in here or in any of the set method because this
        // object may be materialized in a remote machine and then sent thru
        // the net to the machine where it will be used.
    }


  //Most of our customers would be using jndi to get the data
  //sources. Since we don't have a jndi to test this, we are
  //adding this method to fake it. This is getting used in
  //xaJNDI test so we can compare the 2 data sources.
    public boolean equals(Object p0) {
        if (!(p0 instanceof EmbeddedDataSource))
            return false;

        EmbeddedDataSource ds = (EmbeddedDataSource)p0;
        if (createDatabase != null) {
            if (!(createDatabase.equals(ds.createDatabase)))
                return false;
        } else if (ds.createDatabase != null)
            return false;

        if (shutdownDatabase != null) {
            if  (!(shutdownDatabase.equals(ds.shutdownDatabase)))
                return false;
        } else if (ds.shutdownDatabase != null)
            return false;

        if (connectionAttributes != null) {
            if  (!(connectionAttributes.equals(ds.connectionAttributes)))
                return false;
        } else if (ds.connectionAttributes != null)
            return false;

        if (loginTimeout != ds.loginTimeout)
            return false;

        return true;
    }


    @Override
    public int hashCode() {
        return Objects.hash(
                createDatabase,
                shutdownDatabase,
                connectionAttributes,
                loginTimeout);
    }

    /*
     * Properties to be seen by Bean - access thru reflection.
     */

    /**
        Set this property to create a new database.  If this
        property is not set, the database (identified by databaseName) is
        assumed to be already existing.

        @param create if set to the string "create", this data source will try
        to create a new database of databaseName, or boot the database if one
        by that name already exists.
     */
    public final void setCreateDatabase(String create) {
        if (create != null && create.toLowerCase(java.util.Locale.ENGLISH).equals("create"))
            createDatabase = create;
        else
            createDatabase = null;
    }
    /** @return "create" if create is set, or null if not */
    public final String getCreateDatabase() {
        return createDatabase;
    }


    /**
         Set this property if one wishes to shutdown the database identified by
        databaseName.

        @param shutdown if set to the string "shutdown", this data source will
        shutdown the database if it is running.
     */
    public final void setShutdownDatabase(String shutdown) {
        if (shutdown != null && shutdown.equalsIgnoreCase("shutdown"))
            shutdownDatabase = shutdown;
        else
            shutdownDatabase = null;
    }
    /** @return "shutdown" if shutdown is set, or null if not */
    public final String getShutdownDatabase() {
        return shutdownDatabase;
    }

    /**
         Set this property to pass in more Derby specific
        connection URL attributes.
        <BR>
       Any attributes that can be set using a property of this DataSource implementation
       (e.g user, password) should not be set in connectionAttributes. Conflicting
       settings in connectionAttributes and properties of the DataSource will lead to
       unexpected behaviour. 

        @param prop set to the list of Derby connection
        attributes separated by semi-colons.   E.g., to specify an encryption
        bootPassword of "x8hhk2adf", and set upgrade to true, do the following:
        <PRE>
            ds.setConnectionAttributes("bootPassword=x8hhk2adf;upgrade=true");
        </PRE>
        See the Derby documentation for complete list.
     */
    public final void setConnectionAttributes(String prop) {
         connectionAttributes = prop;
         update();
    }
    /** @return Derby specific connection URL attributes */
    public final String getConnectionAttributes() {
        return connectionAttributes;
    }


    /**
        Set attributeAsPassword property to enable passing connection request attributes in the password argument of getConnection.
        If the property is set to true then the password argument of the DataSource.getConnection(String user, String password)
        method call is taken to be a list of connection attributes with the same format as the connectionAttributes property.

        @param attributesAsPassword true to encode password argument as a set of connection attributes in a connection request.
    */
    public final void setAttributesAsPassword(boolean attributesAsPassword) {
        this.attributesAsPassword = attributesAsPassword;
         update();
    }

    /**
        Return the value of the attributesAsPassword property.
    */
    public final boolean getAttributesAsPassword() {
        return attributesAsPassword;
    }

    /*
     * DataSource methods - keep these non-final so that others can
     * extend Derby's classes if they choose to.
     */


    /**
     * Attempt to establish a database connection.
     *
     * @return  a Connection to the database
     * @exception SQLException if a database-access error occurs.
     */
    public Connection getConnection() throws SQLException
    {
        return this.getConnection(getUser(), getPassword(), false);
    }

    /**
     * Attempt to establish a database connection with the given username and password.
       If the attributeAsPassword property is set to true then the password argument is taken to be a list of
       connection attributes with the same format as the connectionAttributes property.

     *
     * @param username the database user on whose behalf the Connection is
     *  being made
     * @param password the user's password
     * @return  a Connection to the database
     * @exception SQLException if a database-access error occurs.
     */
    public Connection getConnection(String username, String password)
         throws SQLException
    {
        return this.getConnection(username, password, true);
    }

//    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException{ throw new SQLFeatureNotSupportedException(); }

//    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException{ throw new UnsupportedOperationException(); }

//    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException{ throw new UnsupportedOperationException(); }

    /**
        @param    requestPassword true if the password came from the getConnection() call.
    */
    final Connection getConnection(String username, String password, boolean requestPassword)
        throws SQLException {

        Properties info = new Properties();
        if (username != null)
            info.put(Attribute.USERNAME_ATTR, username);

        if (!requestPassword || !attributesAsPassword)
        {
            if (password != null)
                info.put(Attribute.PASSWORD_ATTR, password);
        }

        if (createDatabase != null)
            info.put(Attribute.CREATE_ATTR, "true");
        if (shutdownDatabase != null)
            info.put(Attribute.SHUTDOWN_ATTR, "true");

        String url = jdbcurl;

        if (attributesAsPassword && requestPassword && password != null) {


            url = url +
                    ';' +
                    password;

        }
        Connection conn =  findDriver().connect(url, info);

    // JDBC driver's getConnection method returns null if
    // the driver does not handle the request's URL.
        if (conn == null)
           throw Util.generateCsSQLException(SQLState.PROPERTY_INVALID_VALUE,Attribute.DBNAME_ATTR,getDatabaseName());

        return conn;
    }
   
    synchronized InternalDriver findDriver() throws SQLException
    {
        String url = jdbcurl;

        // The driver has either never been booted, or it has been
        // shutdown by a 'jdbc:splice:;shutdown=true'
        if (driver == null || !driver.acceptsURL(url))
        {

            new EmbeddedDriver();

            // If we know the driver, we loaded it.   Otherwise only
            // work if DriverManager has already loaded it.
            // DriverManager will throw an exception if driver is not found
            Driver registerDriver = DriverManager.getDriver(url);
            if (registerDriver instanceof AutoloadedDriver) {
                driver = (InternalDriver) AutoloadedDriver.getDriverModule();
            } else {
                driver = (InternalDriver) registerDriver;
            }
        }
        return driver;
        // else driver != null and driver can accept url
    }

    void update()
    {
        StringBuilder sb = new StringBuilder(64);

        sb.append(Attribute.PROTOCOL);


        // Set the database name from the databaseName property
        String dbName = getDatabaseName();

        if (dbName != null) {
            dbName = dbName.trim();
        }

        if (dbName == null || dbName.isEmpty()) {
            // need to put something in so that we do not allow the
            // database name to be set from the request or from the
            // connection attributes.

            // this space will selected as the database name (and trimmed to an empty string)
            // See the getDatabaseName() code in InternalDriver. Since this is a non-null
            // value, it will be selected over any databaseName connection attribute.
            dbName = " ";
        }

        sb.append(dbName);


        String connAttrs = getConnectionAttributes();
        if (connAttrs != null) {
            connAttrs = connAttrs.trim();
            if (!connAttrs.isEmpty()) {
                sb.append(';');
                sb.append(connectionAttributes);
            }
        }

        jdbcurl = sb.toString();
    }
}
