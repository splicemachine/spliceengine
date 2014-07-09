
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

/** Example resource class hosted at the URI path "/myresource"
 */
@Path("/myresource")
public class MyResource {

    /** Method processing HTTP GET requests, producing "text/plain" MIME media
     * type.
     * @return String that will be send back as a response of type "text/plain".
     */
    @GET
    @Path("/hello")
    @Produces(MediaType.TEXT_PLAIN)
    public String getIt() {
        return "Hi there!";
    }

    /** Method processing HTTP GET requests, producing "text/plain" MIME media
     * type.
     * RESTful service that returns JSON with the traced statement plan, which is a SQL operation tree, for the statement.
     * @return String that will be send back as a response of type "text/plain".
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

    /** Method processing HTTP GET requests, producing "text/plain" MIME media
     * type.
     * @return String that will be send back as a response of type "text/plain".
     */
    @GET
    @Path("/sql2js")
    @Produces(MediaType.APPLICATION_JSON)
    public List<Map<String, String>> getQueryResultsAsJavaScriptObjects(@QueryParam("query")String query) {

    	// Get the "raw" JDBC results stored in lists and maps.
    	Map<String, List> map = getSQLResults(query);
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

    /** Method processing HTTP GET requests, producing "text/plain" MIME media
     * type.
     * @return String that will be send back as a response of type "text/plain".
     */
    @GET
    @Path("/sql2jdbc")
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, List> getQueryResultsAsJDBCObjects(@QueryParam("query")String query) {
        return getSQLResults(query);
    }

	/**
	 * 
	 */
	private Map<String, List> getSQLResults(String query) {
		if (query == null) {
			query = "SELECT * FROM SYS.SYSTABLES";
		}
		Connection conn = null;
        Statement stmt = null;
        try {
        	Class.forName("org.apache.derby.jdbc.ClientDriver").newInstance();
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

	private List<String> getHeaders(ResultSet rs) throws SQLException, java.io.IOException {
		ResultSetMetaData md = rs.getMetaData();
		int colCount = md.getColumnCount();
		List<String> headers = new ArrayList<String>(colCount); 
		for (int i = 1; i <= colCount; i++) {
			headers.add(md.getColumnLabel(i));
		}
		return headers;
	}

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
