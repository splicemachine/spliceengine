package com.splicemachine.rest.services;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

/**
 * Splice RESTful SQL resources hosted at the URI path "/sqlresource".
 */
@Path("/sqlresource")
public class SQLResource {

	/**
	 * Switch for fake debug cluster.  Randomly report up/down status for a 5 node cluster.
	 * Helpful for testing the green/red status of a cluster of Splice servers.
	 */
	private static boolean isFakeCluster = false;

	/**
	 * JNDI names for the configured values for the primary JDBC connection.
	 */
    public static String JNDI_NAME_DRIVER   = "java:comp/env/driver";
    public static String JNDI_NAME_HOST     = "java:comp/env/host";
    public static String JNDI_NAME_PORT     = "java:comp/env/port";
    public static String JNDI_NAME_USER     = "java:comp/env/user";
    public static String JNDI_NAME_PASSWORD = "java:comp/env/password";

	/**
	 * Defaults for the primary Splice JDBC connection.
	 */
	public static String DEFAULT_DRIVER   = "org.apache.derby.jdbc.ClientDriver";
	public static String DEFAULT_HOST     = "localhost";
	public static String DEFAULT_PORT     = "1527";
	public static String DEFAULT_USER     = "app";
	public static String DEFAULT_PASSWORD = "app";

	/**
	 * Primary Splice JDBC connection.
	 */
	private static String driver;
	private static String host;
	private static String port;
	private static String user;
	private static String password;

	/**
	 * Static initializer.
	 */
	static {
		init();
	}

	/**
	 * Initialize the primary JDBC connection from JNDI.
	 */
    private static void init() {
		try {
		    InitialContext ic = new InitialContext();
		    driver   = getValueFromJNDI(ic, JNDI_NAME_DRIVER, DEFAULT_DRIVER);
		    host     = getValueFromJNDI(ic, JNDI_NAME_HOST, DEFAULT_HOST);
		    port     = getValueFromJNDI(ic, JNDI_NAME_PORT, DEFAULT_PORT);
		    user     = getValueFromJNDI(ic, JNDI_NAME_USER, DEFAULT_USER);
		    password = getValueFromJNDI(ic, JNDI_NAME_PASSWORD, DEFAULT_PASSWORD);
		} catch (NamingException e) {
			// TODO: Add ERROR level logging statement.
        	throw new RuntimeException("Problem creating javax.naming.InitialContext instance", e);
		}
	}

	/**
	 * @param ic
	 * @throws NamingException
	 */
	private static String getValueFromJNDI(InitialContext ic, String jndiName, String defaultValue) {
		try {
			String value = (String)ic.lookup(jndiName);
			// TODO: Change to TRACE level logging statement.
			System.out.println(jndiName + " is bound to: " + value);
			return (value == null ? defaultValue : value);
		} catch (NamingException e) {
			// TODO: Change to INFO level logging statement.
			System.err.println(String.format("Problem looking up '%s'.  Value may be missing from jetty.xml, so using default value of %s", jndiName, defaultValue));
			return defaultValue;
		}
	}

	/**
	 * Return the driver for the primary Splice JDBC connection.
	 * @return the driver
	 */
	public static String getDriver() {
		return driver;
	}

	/**
	 * Set the driver for the primary Splice JDBC connection.
	 * @param driver the driver to set
	 */
	public static void setDriver(String driver) {
		SQLResource.driver = driver;
	}

	/**
	 * Return the host for the primary Splice JDBC connection.
	 * @return the host
	 */
	public static String getHost() {
		return host;
	}

	/**
	 * Set the host for the primary Splice JDBC connection.
	 * @param host the host to set
	 */
	public static void setHost(String host) {
		SQLResource.host = host;
	}

	/**
	 * Return the port for the primary Splice JDBC connection.
	 * @return the port
	 */
	public static String getPort() {
		return port;
	}

	/**
	 * Set the port for the primary Splice JDBC connection.
	 * @param port the port to set
	 */
	public static void setPort(String port) {
		SQLResource.port = port;
	}

	/**
	 * Return the user for the primary Splice JDBC connection.
	 * @return the user
	 */
	public static String getUser() {
		return user;
	}

	/**
	 * Set the user for the primary Splice JDBC connection.
	 * @param user the user to set
	 */
	public static void setUser(String user) {
		SQLResource.user = user;
	}

	/**
	 * Return the password for the primary Splice JDBC connection.
	 * @return the password
	 */
	public static String getPassword() {
		return password;
	}

	/**
	 * Set the password for the primary Splice JDBC connection.
	 * @param password the password to set
	 */
	public static void setPassword(String password) {
		SQLResource.password = password;
	}

	/**
	 * RESTful service that returns a JSON document containing the traced statement plan (SQL operation tree)
	 * for the specified statement.
	 * TODO: Add example of what the JSON would look like.
     * @param statementId	STATEMENTID from SYS.SYSSTATEMENTHISTORY table in a Splice database
	 * @return String with a response type of "application/json".
	 */
	@GET
	@Path("/tracedStatements/{statementId}")
	@Produces(MediaType.APPLICATION_JSON)
	public String getTracedStatementTree(@PathParam("statementId")String statementId) {
		// TODO: Prepared statements should be used.
		if (statementId == null) return null;
		List<Map<String, String>> objects = getQueryResultsAsJavaScriptObjects(String.format("call syscs_util.syscs_get_xplain_trace(%s, 1, 'json')", statementId));
		if (objects == null) return null;
		Map<String, String> object = objects.get(0);
		if (object == null) return null;
		return object.get("PLAN");
	}

	/**
	 * RESTful service that returns a JSON document containing the "up" status of all Splice servers.
	 * TODO: Add example of what the JSON would look like.
	 * @return Map of objects that are transformed into a response type of "application/json".
	 */
	@GET
	@Path("/servers/status")
	@Produces(MediaType.APPLICATION_JSON)
	public Map<String, Object> getServersStatus() {

		List<Map<String, String>> objects = getQueryResultsAsJavaScriptObjects("CALL SYSCS_UTIL.SYSCS_GET_ACTIVE_SERVERS()");
		if (objects == null) return null;

		// Add some more fake servers for UI testing.
		if (isFakeCluster) {
			if (objects.size() > 0 && objects.get(0) != null) {
				objects.add(new HashMap(objects.get(0)));
				objects.add(new HashMap(objects.get(0)));
				objects.add(new HashMap(objects.get(0)));
				objects.add(new HashMap(objects.get(0)));
			}
		}

		// Count the servers that are up/down and store their status in the respective server object.
		// Also, move PORT into RPCPORT (for HBase) and add the JDBC port (hard coded to 1527 currently).
		int upCount = 0, downCount = 0;
		for (Map<String, String> object : objects) {
			if (object != null) {
				boolean upStatus = isServerUp(object);
				if (upStatus) {
					upCount++;
				} else {
					downCount++;
				}
				object.put("UP", Boolean.toString(upStatus));
				object.put("RPCPORT", object.remove("PORT"));
				object.put("JDBCPORT", getPort());
			}
		}

    	Map<String, Object> objectMap = new HashMap<String, Object>(3);
    	objectMap.put("servers", objects);
    	objectMap.put("serverUpCount", upCount);
    	objectMap.put("serverDownCount", downCount);

    	// TODO: Add dead servers to down count.
		return objectMap;
	}

	private boolean isServerUp(Map<String, String> serverObject) {

		// Return random up/down status for UI testing.
		if (isFakeCluster) {
			return (Math.random() < 0.5 ? true : false);
		}

		// TODO: Make this a lighter test.  There is no need to create a bunch of lists and maps for a simple test
		// that may get run over and over repeatedly.
		// TODO: Add log4j and log all of the cases where false is returned due to a null value.
		if (serverObject == null) return false;
		String host = serverObject.get("HOSTNAME");
		if (host == null) return false;

		// Don't use the port from the server object since that is the HBase region server RPC port.
		// Splice only supports listening to port 1527 currently, so use the configured port for forward compatibility.
    	Map<String, List> map = getQueryResults("select tabletype from sys.systables {limit 1}", host, getPort());
    	if (map == null) return false;
    	List<String> headers = map.get("headers");
    	List<List<String>> rows = map.get("rows");
    	if (headers == null || rows == null) return false;
    	if (headers.size() == 0 || rows.size() == 0) return false;

		return true;
	}

	/**
	 * RESTful service that queries a Splice database with the specified SQL query and returns the results as a
	 * JSON document.  The JSON document is an array of "object literals" where each object maps directly to a
	 * row in the result set.
	 * TODO: Add example of what the JSON would look like.
     * @param query	SQL select statement
	 * @return List of Map objects that are transformed into a response type of "application/json".
	 */
    @GET
    @Path("/query2js")
    @Produces(MediaType.APPLICATION_JSON)
    public List<Map<String, String>> getQueryResultsAsJavaScriptObjects(@QueryParam("query")String query) {

    	// Get the "raw" JDBC results stored in lists and maps.
    	Map<String, List> map = getQueryResults(query, getHost(), getPort());
    	if (map == null) return null;

    	List<String> headers = map.get("headers");
    	List<List<String>> rows = map.get("rows");
    	if (headers == null || rows == null) return null;

    	// Transform the results into a format consumable by JavaScript.
    	List<Map<String, String>> objectsList = new ArrayList<Map<String, String>>(rows.size());
    	int numCols = headers.size();
    	for (List<String> row : rows) {
	    	Map<String, String> objectMap = new HashMap<String, String>(numCols);
    		int colIndex = 0;
	    	for (String columnName : headers) {
		    	objectMap.put(columnName, row.get(colIndex));
		    	colIndex++;
	    	}
	    	objectsList.add(objectMap);
    	}

    	return objectsList;
    }

	/**
	 * RESTful service that queries a Splice database with the specified SQL query and returns the results as a
	 * JSON document.  The JSON document matches what JDBC returns.  There is a "headers" array which is an array of
	 * strings with the names of the columns being returned.  And there is a "rows" array which is an array of "rows",
	 * where each row is an array of the column values being returned.
	 * TODO: Add example of what the JSON would look like.
     * @param query	SQL select statement
	 * @return Map of "headers" and "rows" arrays that are transformed into a response type of "application/json".
	 */
    @GET
    @Path("/query2jdbc")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, List> getQueryResultsAsJDBCObjects(@QueryParam("query")String query) {
        return getQueryResults(query, getHost(), getPort());
    }

    /**
	 * Worker method that returns the results from a SQL query in JSON that closely matches what JDBC returns with
	 * the ResultSetMetaData and ResultSet objects.
     * @param query	SQL select statement
	 * @return Map of "headers" and "rows" lists
     */
	private static Map<String, List> getQueryResults(String query, String host, String port) {
		if (query == null) {
			// TODO: Remove this default and throw the appropriate exception or just null/empty map/map with empty arrays.
			query = "SELECT * FROM SYS.SYSTABLES";
		}
		if (host == null) {
			host = getHost();
		}
		if (port == null) {
			port = getPort();
		}
		Connection conn = null;
        Statement stmt = null;
        try {
        	Class.forName(getDriver()).newInstance();
        	// TODO: Add connection pooling.
        	String jdbcURL = String.format("jdbc:splice://%s:%s/splicedb;create=true", host, port);
        	conn = DriverManager.getConnection(jdbcURL, getUser(), getPassword());
        	stmt = conn.createStatement();
        	ResultSet rs = stmt.executeQuery(query);
        	List<String> queries = new ArrayList<String>(1);
        	queries.add(query);
        	List<String> headers = getHeaders(rs);
        	List<List<String>> rows = getRows(rs);
        	Map<String, List> map = new HashMap<String, List>();
        	map.put("queries", queries);
        	map.put("headers", headers);
        	map.put("rows", rows);
        	return map;
        } catch (Exception e) {
        	throw new RuntimeException(e);
        } finally {
        	try {
        		if (stmt != null) {
        			stmt.close();
        		}
        	} catch (SQLException e) {
            	throw new RuntimeException(e);
        	}
        	try {
        		if (conn != null) {
        			conn.close();
        		}
        	} catch (SQLException e) {
            	throw new RuntimeException(e);
        	}
        }
	}

	/**
	 * Worker method that transforms the column labels from JDBC's ResultSetMetaData.getColumnLabel(i) into the "headers" array,
	 * which is an array of strings with the column labels (headers).
	 * @param rs JDBC ResultSet
	 * @return array of strings with the column labels (headers)
	 * @throws SQLException
	 * @throws java.io.IOException
	 */
	private static List<String> getHeaders(ResultSet rs) throws SQLException, java.io.IOException {
		ResultSetMetaData md = rs.getMetaData();
		int colCount = md.getColumnCount();
		List<String> headers = new ArrayList<String>(colCount); 
		for (int i = 1; i <= colCount; i++) {
			headers.add(md.getColumnLabel(i));
		}
		return headers;
	}

	/**
	 * Worker method that transforms the rows from JDBC's ResultSet.getString(i) into the "rows" array,
	 * which is an array of "rows" where every row is an array of its column values.
	 * @param rs	a JDBC ResultSet
	 * @return array of "rows" where every row is an array of its column values
	 * @throws SQLException
	 * @throws java.io.IOException
	 */
	private static List<List<String>> getRows(ResultSet rs) throws SQLException, java.io.IOException {
		ResultSetMetaData md = rs.getMetaData();
		int colCount = md.getColumnCount();
		List<List<String>> rows = new ArrayList<List<String>>(); 
		while (rs.next()) {
			List<String> row = new ArrayList<String>(colCount); 
			for (int i = 1; i <= colCount; i++) {
				row.add(rs.getString(i));
			}
			rows.add(row);
		}
		return rows;
	}
}
