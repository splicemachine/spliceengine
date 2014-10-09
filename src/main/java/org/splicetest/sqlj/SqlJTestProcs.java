package org.splicetest.sqlj;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Stored procedures to test the SQLJ JAR file loading system procedures (INSTALL_JAR, REPLACE_JAR, and REMOVE_JAR).
 *
 * @author David Winters
 */
public class SqlJTestProcs {

	/*
	-- Install the JAR file into the database and add it to the CLASSPATH of the database.
	CALL SQLJ.INSTALL_JAR('/Users/dwinters/Documents/workspace/sqlj-it-procs/target/sqlj-it-procs-1.0-SNAPSHOT.jar', 'APP.SQLJ_IT_PROCS_JAR', 0);
	CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.classpath', 'APP.SQLJ_IT_PROCS_JAR');
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
		CREATE PROCEDURE APP.SIMPLE_ONE_ARG_PROC(IN name VARCHAR(30)) PARAMETER STYLE JAVA READS SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'org.splicetest.sqlj.SqlJTestProcs.SIMPLE_ONE_ARG_PROC';
		CALL APP.SIMPLE_ONE_ARG_PROC('FOOBAR');
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
		CREATE PROCEDURE APP.SIMPLE_NO_ARGS_PROC() PARAMETER STYLE JAVA READS SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'org.splicetest.sqlj.SqlJTestProcs.SIMPLE_NO_ARGS_PROC';
		CALL APP.SIMPLE_NO_ARGS_PROC();
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
		CREATE PROCEDURE APP.GET_PROC_COLS(IN procName VARCHAR(30)) PARAMETER STYLE JAVA READS SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'org.splicetest.sqlj.SqlJTestProcs.GET_PROC_COLS';
		CALL APP.GET_PROC_COLS('GET_PROC_COLS');
		 */
		String catalog = null;
		String schemaPattern = null;
		String procedureNamePattern = procName;
		String columnNamePattern = null;
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		DatabaseMetaData dbMeta = conn.getMetaData();
		ResultSet rsProcCols = dbMeta.getProcedureColumns(catalog, schemaPattern, procedureNamePattern, columnNamePattern);
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
		CREATE PROCEDURE APP.THROW_SQL_EXCEPTION(IN throwException BOOLEAN) PARAMETER STYLE JAVA READS SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'org.splicetest.sqlj.SqlJTestProcs.THROW_SQL_EXCEPTION';
		CALL APP.THROW_SQL_EXCEPTION(true);
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
		CREATE PROCEDURE APP.THROW_RUNTIME_EXCEPTION(IN throwException BOOLEAN) PARAMETER STYLE JAVA READS SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'org.splicetest.sqlj.SqlJTestProcs.THROW_RUNTIME_EXCEPTION';
		CALL APP.THROW_RUNTIME_EXCEPTION(true);
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
}
