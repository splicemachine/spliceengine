/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package org.splicetest.sqlj;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Stored procedures to test the SQLJ JAR file loading system procedures (INSTALL_JAR, REPLACE_JAR, and REMOVE_JAR).
 *
 * @see com.splicemachine.derby.impl.sql.catalog.SqlJJarIT
 *
 * @author David Winters
 */
public class SqlJTestProcs {

	/*
	-- Install the JAR file into the database and add it to the CLASSPATH of the database.
	CALL SQLJ.INSTALL_JAR('/Users/dwinters/Documents/workspace3/sqlj-it-procs/target/sqlj-it-procs-1.0.1-SNAPSHOT.jar', 'SPLICE.SQLJ_IT_PROCS_JAR', 0);
	CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.classpath', 'SPLICE.SQLJ_IT_PROCS_JAR');
	-- Replace the JAR file.
	CALL SQLJ.REPLACE_JAR('/Users/dwinters/Documents/workspace3/sqlj-it-procs/target/sqlj-it-procs-1.0.1-SNAPSHOT.jar', 'SPLICE.SQLJ_IT_PROCS_JAR');
	 */

	/**
	 * Test stored procedure that accepts one argument.
	 *
	 * @param name     name of something
	 * @param rs       output parameter, the result set object containing the result
	 */
	public static void SIMPLE_ONE_ARG_PROC(String name, ResultSet[] rs)
		throws SQLException
	{
		/*
		-- Declare and execute the procedure in ij.
		CREATE PROCEDURE SPLICE.SIMPLE_ONE_ARG_PROC(IN name VARCHAR(30)) PARAMETER STYLE JAVA READS SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'org.splicetest.sqlj.SqlJTestProcs.SIMPLE_ONE_ARG_PROC';
		CALL SPLICE.SIMPLE_ONE_ARG_PROC('FOOBAR');
		 */
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		PreparedStatement ps = conn.prepareStatement("select tableid, tablename from SYS.SYSTABLES");
		rs[0] = ps.executeQuery();
		conn.close();
	}

	/**
	 * Test stored procedure that accepts no arguments.
	 *
	 * @param rs       output parameter, the result set object containing the result
	 */
	public static void SIMPLE_NO_ARGS_PROC(ResultSet[] rs)
		throws SQLException
	{
		/*
		-- Declare and execute the procedure in ij.
		CREATE PROCEDURE SPLICE.SIMPLE_NO_ARGS_PROC() PARAMETER STYLE JAVA READS SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'org.splicetest.sqlj.SqlJTestProcs.SIMPLE_NO_ARGS_PROC';
		CALL SPLICE.SIMPLE_NO_ARGS_PROC();
		 */
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		PreparedStatement ps = conn.prepareStatement("select tableid, tablename from SYS.SYSTABLES");
		rs[0] = ps.executeQuery();
		conn.close();
	}

	/**
	 * Get the columns for a stored procedure.
	 *
	 * @param name     name of procedure
	 * @param rs       columns for procedure
	 */
	public static void GET_PROC_COLS(String procName, ResultSet[] rs)
		throws SQLException
	{
		/*
		-- Declare and execute the procedure in ij.
		CREATE PROCEDURE SPLICE.GET_PROC_COLS(IN procName VARCHAR(30)) PARAMETER STYLE JAVA READS SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'org.splicetest.sqlj.SqlJTestProcs.GET_PROC_COLS';
		CALL SPLICE.GET_PROC_COLS('GET_PROC_COLS');
		 */
		String catalog = null;
		String schemaPattern = null;
		String columnNamePattern = null;
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		DatabaseMetaData dbMeta = conn.getMetaData();
		ResultSet rsProcCols = dbMeta.getProcedureColumns(catalog, schemaPattern, procName, columnNamePattern);
		rs[0] = rsProcCols;
		conn.close();
	}

	/**
	 * Test stored procedure that throws a JDBC SQLException.
	 *
	 * @param throwException    throw an exception if true
	 * @param rs                output parameter, the result set object containing the result
	 */
	public static void THROW_SQL_EXCEPTION(boolean throwException, ResultSet[] rs)
		throws SQLException
	{
		/*
		-- Declare and execute the procedure in ij.
		CREATE PROCEDURE SPLICE.THROW_SQL_EXCEPTION(IN throwException BOOLEAN) PARAMETER STYLE JAVA READS SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'org.splicetest.sqlj.SqlJTestProcs.THROW_SQL_EXCEPTION';
		CALL SPLICE.THROW_SQL_EXCEPTION(true);
		 */
		if (throwException) {
			throw new SQLException("Pretend that something bad happened");
		}
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		PreparedStatement ps = conn.prepareStatement("values(1,2,3), (4,5,6)");
		rs[0] = ps.executeQuery();
		conn.close();
	}

	/**
	 * Test stored procedure that throws a Java RuntimeException.
	 *
	 * @param throwException    throw an exception if true
	 * @param rs                output parameter, the result set object containing the result
	 */
	public static void THROW_RUNTIME_EXCEPTION(boolean throwException, ResultSet[] rs)
		throws RuntimeException
	{
		/*
		-- Declare and execute the procedure in ij.
		CREATE PROCEDURE SPLICE.THROW_RUNTIME_EXCEPTION(IN throwException BOOLEAN) PARAMETER STYLE JAVA READS SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'org.splicetest.sqlj.SqlJTestProcs.THROW_RUNTIME_EXCEPTION';
		CALL SPLICE.THROW_RUNTIME_EXCEPTION(true);
		 */
		if (throwException) {
			throw new RuntimeException("Pretend that something bad happened");
		}
		try {
			Connection conn = DriverManager.getConnection("jdbc:default:connection");
			PreparedStatement ps = conn.prepareStatement("values(1,2,3), (4,5,6)");
			rs[0] = ps.executeQuery();
			conn.close();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public static void JPROC_TYPE_UNIT_TEST(ResultSet[] rs)
			throws Exception {
		//-- Declare and execute the procedure in ij.
		//CREATE PROCEDURE SPLICE.JPROC_TYPE_UNIT_TEST() PARAMETER STYLE JAVA READS SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'com.splicemachine.derby.impl.sql.pyprocedure.PyStoredProcedureResultSetFactoryIT.JPROC_TYPE_UNIT_TEST';
		//CALL SPLICE.JPROC_TYPE_UNIT_TEST();
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		Statement stmt = conn.createStatement();
		rs[0] = stmt.executeQuery("SELECT * FROM TEST_TABLE {limit 1}");
		conn.close();
	}

	/**
	 * Truncates a string to the number of words specified.  An input of
	 * "Today is a wonderful day and I am looking forward to going to the beach.", 5
	 * will return "Today is a wonderful day".
	 *
	 * @param inboundSentence
	 * @param numberOfWords
	 * @return
	 */
	public static String wordLimiter(String inboundSentence, int numberOfWords) {
		String truncatedString = "";
		if(inboundSentence != null) {
			String[] splitBySpace = inboundSentence.split("\\s+");
			if(splitBySpace.length <= numberOfWords) {
				truncatedString = inboundSentence;
			} else {
				StringBuilder sb = new StringBuilder();
				for(int i=0; i<numberOfWords; i++) {
					if(i > 0) sb.append(" ");
					sb.append(splitBySpace[i]);
				}
				truncatedString = sb.toString();
			}
		}
		return truncatedString;
	}

	// a simple udf function
	public static int MySum(int a, int b) {
		return a+b;
	}
	/**
	* List that tracks calls to {@code intProcedure()}. It is used to verify
	* that triggers have fired.
	*/
	public static List<Integer> procedureCalls = new ArrayList<Integer>();

	/**
	* A procedure that takes an {@code int} argument and adds it to the
	* {@link #procedureCalls} list. Can be used as a stored procedure to
	* verify that a trigger has been called. Particularly useful in BEFORE
	* triggers, as they are not allowed to modify SQL data.
	*
	* @param i an integer
	*/
	public static void intProcedure(int i) {
	procedureCalls.add(i);
	}


	/**
	* Stored function used by {@link #testFunctionReadsSQLData()}. It
	* checks whether the given table is empty.
	*
	* @param table the table to check
	* @return {@code true} if the table is empty, {@code false} otherwise
	*/
	public static boolean tableIsEmpty(String table) throws SQLException {
		Connection c = DriverManager.getConnection("jdbc:splice://localhost:1527/splicedb;user=splice;password=admin");
		Statement s = c.createStatement();
		ResultSet rs = s.executeQuery("select * from " + table);
		boolean empty = !rs.next();

		rs.close();
		s.close();
		c.close();

		return empty;
	}
}
