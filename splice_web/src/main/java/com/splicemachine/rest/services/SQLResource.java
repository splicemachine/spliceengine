package com.splicemachine.rest.services;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
	 * RESTful service that returns a JSON document containing the traced statement plan (SQL operation tree)
	 * for the specified statement.
     * @param statementId	STATEMENTID from SYS.SYSSTATEMENTHISTORY table in a Splice database
	 * @return String with a response type of "application/json".
	 */
	@GET
	@Path("/tracedStatements/{statementId}")
	@Produces(MediaType.APPLICATION_JSON)
	public String getTracedStatementTree(@PathParam("statementId")String statementId) {
		// TODO: This method is a quick hack to get some data flowing.
		// Prepared statements should be used at a minimum.
		if (statementId == null) return null;
		List<Map<String, String>> objects = getQueryResultsAsJavaScriptObjects(String.format("call syscs_util.syscs_get_xplain_trace(%s, 1, 'json')", statementId));
		if (objects == null) return null;
		Map<String, String> object = objects.get(0);
		if (object == null) return null;
		return object.get("PLAN");
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
    	Map<String, List> map = getQueryResults(query);
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
        return getQueryResults(query);
    }

    /**
	 * Worker method that returns the results from a SQL query in JSON that closely matches what JDBC returns with
	 * the ResultSetMetaData and ResultSet objects.
     * @param query	SQL select statement
	 * @return Map of "headers" and "rows" lists
     */
	private Map<String, List> getQueryResults(String query) {
		if (query == null) {
			// TODO: Remove this default and throw the appropriate exception.
			query = "SELECT * FROM SYS.SYSTABLES";
		}
		Connection conn = null;
        Statement stmt = null;
        try {
        	Class.forName("org.apache.derby.jdbc.ClientDriver").newInstance();
        	// TODO: Remove hard coded JDBC URL.  Pull URL from config.  Use same config module as HBase uses (*-site.xml, *-default.xml).
        	// TODO: Add connection pooling.
        	conn = DriverManager.getConnection("jdbc:splice://localhost:1527/splicedb;create=true", "app", "app");
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
	private List<String> getHeaders(ResultSet rs) throws SQLException, java.io.IOException {
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
	private List<List<String>> getRows(ResultSet rs) throws SQLException, java.io.IOException {
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
