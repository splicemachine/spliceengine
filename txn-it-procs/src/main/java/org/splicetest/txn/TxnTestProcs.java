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

package org.splicetest.txn;

import com.splicemachine.db.iapi.error.PublicAPI;
import java.sql.*;

import com.splicemachine.db.impl.sql.pyprocedure.PyCodeUtil;
import com.splicemachine.db.impl.sql.pyprocedure.PyInterpreterPool;
import com.splicemachine.pipeline.Exceptions;
import org.python.util.PythonInterpreter;
import org.python.core.*;
import com.splicemachine.db.impl.sql.pyprocedure.PyStoredProcedureResultSetFactory;

/**
 * Stored procedures to test the transactional correctness of the Splice Machine stored procedure execution framework.
 * This class contains stored procedures that will be dynamically loaded into the Splice Machine
 * database with the SQLJ jar file loading system procedures.  If your tests require a custom stored procedure,
 * you can add it to this file and load it into your 'IT' test with the SQLJ jar file loading system procedures.
 *
 * @see com.splicemachine.derby.transactions.CallableTransactionIT
 *
 * @author David Winters
 */
public class TxnTestProcs {

	/*
	-- Install the JAR file into the database and add it to the CLASSPATH of the database.
	CALL SQLJ.INSTALL_JAR('/Users/dwinters/Documents/workspace3/txn-it-procs/target/txn-it-procs-1.0-SNAPSHOT.jar', 'SPLICE.TXN_IT_PROCS_JAR', 0);
	CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('derby.database.classpath', 'SPLICE.TXN_IT_PROCS_JAR');
	-- Replace the JAR file.
	CALL SQLJ.REPLACE_JAR('/Users/dwinters/Documents/workspace3/txn-it-procs/target/txn-it-procs-1.0-SNAPSHOT.jar', 'SPLICE.TXN_IT_PROCS_JAR');
	 */

	/*
	 * ============================================================================================
	 * The following test the transactional correctness of Splice stored procedures.
	 * These are the Original Testing Procs
	 * ============================================================================================
	 */

	/**
	 * Create the EMPLOYEE table.
	 */
	public static void CREATE_EMPLOYEE_TABLE(String tableName)
		throws SQLException
	{
		/*
		-- Declare and execute the procedure in ij.
		CREATE PROCEDURE SPLICE.CREATE_EMPLOYEE_TABLE(IN tableName VARCHAR(40)) PARAMETER STYLE JAVA MODIFIES SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 0 EXTERNAL NAME 'org.splicetest.txn.TxnTestProcs.CREATE_EMPLOYEE_TABLE';
		CALL SPLICE.CREATE_EMPLOYEE_TABLE('EMPLOYEE');
		 */
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		createEmployeeTable(conn, tableName);
		conn.close();
	}

	/**
	 * Drop the EMPLOYEE table.
	 */
	public static void DROP_EMPLOYEE_TABLE(String tableName)
		throws SQLException
	{
		/*
		-- Declare and execute the procedure in ij.
		CREATE PROCEDURE SPLICE.DROP_EMPLOYEE_TABLE(IN tableName VARCHAR(40)) PARAMETER STYLE JAVA MODIFIES SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 0 EXTERNAL NAME 'org.splicetest.txn.TxnTestProcs.DROP_EMPLOYEE_TABLE';
		CALL SPLICE.DROP_EMPLOYEE_TABLE('EMPLOYEE');
		 */
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		dropEmployeeTable(conn, tableName);
		conn.close();
	}

	/**
	 * Insert the record for an employee.
	 *
	 * @param id       ID of employee
	 * @param fname    first name of employee
	 * @param lname    last name of employee
	 */
	public static void INSERT_EMPLOYEE(String tableName, Integer id, String fname, String lname)
		throws SQLException
	{
		/*
		-- Declare and execute the procedure in ij.
		CREATE PROCEDURE SPLICE.INSERT_EMPLOYEE(IN tableName VARCHAR(40), IN id INT, IN fname VARCHAR(20), IN lname VARCHAR(30)) PARAMETER STYLE JAVA MODIFIES SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 0 EXTERNAL NAME 'org.splicetest.txn.TxnTestProcs.INSERT_EMPLOYEE';
		CALL SPLICE.INSERT_EMPLOYEE('EMPLOYEE', 2, 'Barney', 'Rubble');
		 */
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		insertEmployee(conn, tableName, id, fname, lname);
		conn.close();
	}

	/**
	 * Get the record for an employee.
	 *
	 * @param id       ID of employee
	 * @param rs       employee record
	 */
	public static void GET_EMPLOYEE(String tableName, Integer id, ResultSet[] rs)
		throws SQLException
	{
		/*
		-- Declare and execute the procedure in ij.
		CREATE PROCEDURE SPLICE.GET_EMPLOYEE(IN tableName VARCHAR(40), IN id INT) PARAMETER STYLE JAVA READS SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'org.splicetest.txn.TxnTestProcs.GET_EMPLOYEE';
		CALL SPLICE.GET_EMPLOYEE('EMPLOYEE', 2);
		 */
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		rs[0] = getEmployeeById(conn, tableName, id);
		conn.close();
	}

	/**
	 * Insert the record for an employee and return it.
	 *
	 * @param id       ID of employee
	 * @param fname    first name of employee
	 * @param lname    last name of employee
	 * @param rs       employee record that was inserted
	 */
	public static void INSERT_AND_GET_EMPLOYEE(String tableName, Integer id, String fname, String lname, ResultSet[] rs)
		throws SQLException
	{
		/*
		-- Declare and execute the procedure in ij.
		CREATE PROCEDURE SPLICE.INSERT_AND_GET_EMPLOYEE(IN tableName VARCHAR(40), IN id INT, IN fname VARCHAR(20), IN lname VARCHAR(30)) PARAMETER STYLE JAVA MODIFIES SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'org.splicetest.txn.TxnTestProcs.INSERT_AND_GET_EMPLOYEE';
		CALL SPLICE.INSERT_AND_GET_EMPLOYEE('EMPLOYEE', 2, 'Barney', 'Rubble');
		 */
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		insertEmployee(conn, tableName, id, fname, lname);
		ResultSet myRS = getEmployeeById(conn, tableName, id);
		rs[0] = myRS;
		conn.close();
	}

	/**
	 * Insert the record for an employee, update it, and return it.
	 *
	 * @param id       ID of employee
	 * @param fname    first name of employee for the INSERT
	 * @param lname    last name of employee for the INSERT
	 * @param fname2   first name of employee for the UPDATE
	 * @param lname2   last name of employee for the UPDATE
	 * @param rs       employee record that was inserted and updated
	 */
	public static void INSERT_UPDATE_AND_GET_EMPLOYEE(String tableName, Integer id, String fname, String lname, String fname2, String lname2, ResultSet[] rs)
		throws SQLException
	{
		/*
		-- Declare and execute the procedure in ij.
		CREATE PROCEDURE SPLICE.INSERT_UPDATE_AND_GET_EMPLOYEE(IN tableName VARCHAR(40), IN id INT, IN fname VARCHAR(20), IN lname VARCHAR(30), IN fname2 VARCHAR(20), IN lname2 VARCHAR(30)) PARAMETER STYLE JAVA MODIFIES SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'org.splicetest.txn.TxnTestProcs.INSERT_UPDATE_AND_GET_EMPLOYEE';
		CALL SPLICE.INSERT_UPDATE_AND_GET_EMPLOYEE('EMPLOYEE', 2, 'Barney', 'Rubble', 'Wilma', 'Flintsone');
		 */
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		insertEmployee(conn, tableName, id, fname, lname);
		updateEmployeeNameById(conn, tableName, id, fname2, lname2);
		rs[0] = getEmployeeById(conn, tableName, id);
		conn.close();
	}

	/**
	 * Insert the record for an employee, update it, execute two employee queries, delete an employee, and return the inserted employee.
	 *
	 * @param id       ID of employee for the INSERT
	 * @param fname    first name of employee for the INSERT
	 * @param lname    last name of employee for the INSERT
	 * @param fname2   first name of employee for the UPDATE
	 * @param lname2   last name of employee for the UPDATE
	 * @param id2      ID of employee for the DELETE
	 * @param rs       employee record that was inserted and updated
	 */
	public static void INSERT_UPDATE_GETx2_DELETE_AND_GET_EMPLOYEE(String tableName, Integer id, String fname, String lname, String fname2, String lname2, Integer id2, ResultSet[] rs)
		throws SQLException
	{
		/*
		-- Declare and execute the procedure in ij.
		CREATE PROCEDURE SPLICE.INSERT_UPDATE_GETx2_DELETE_AND_GET_EMPLOYEE(IN tableName VARCHAR(40), IN id INT, IN fname VARCHAR(20), IN lname VARCHAR(30), IN fname2 VARCHAR(20), IN lname2 VARCHAR(30), IN id2 INT) PARAMETER STYLE JAVA MODIFIES SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'org.splicetest.txn.TxnTestProcs.INSERT_UPDATE_GETx2_DELETE_AND_GET_EMPLOYEE';
		CALL SPLICE.INSERT_UPDATE_GETx2_DELETE_AND_GET_EMPLOYEE('EMPLOYEE', 2, 'Barney', 'Rubble', 'Wilma', 'Flintsone', 3);
		 */
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		insertEmployee(conn, tableName, id, fname, lname);
		updateEmployeeNameById(conn, tableName, id, fname2, lname2);
		getEmployeeFirstNames(conn, tableName);
		getEmployeeLastNames(conn, tableName);
		deleteEmployee(conn, tableName, id2);
		rs[0] = getEmployeeById(conn, tableName, id);
		conn.close();
	}

	/**
	 * Create the EMPLOYEE table, insert a record for an employee, and return it.
	 *
	 * @param id       ID of employee
	 * @param fname    first name of employee
	 * @param lname    last name of employee
	 * @param rs       employee record that was inserted
	 */
	public static void CREATE_INSERT_AND_GET_EMPLOYEE(String tableName, Integer id, String fname, String lname, ResultSet[] rs)
		throws SQLException
	{
		/*
		-- Declare and execute the procedure in ij.
		CREATE PROCEDURE SPLICE.CREATE_INSERT_AND_GET_EMPLOYEE(IN tableName VARCHAR(40), IN id INT, IN fname VARCHAR(20), IN lname VARCHAR(30)) PARAMETER STYLE JAVA MODIFIES SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'org.splicetest.txn.TxnTestProcs.CREATE_INSERT_AND_GET_EMPLOYEE';
		CALL SPLICE.CREATE_INSERT_AND_GET_EMPLOYEE('EMPLOYEE', 1, 'Fred', 'Flintstone');
		 */
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		createEmployeeTable(conn, tableName);
		insertEmployee(conn, tableName, id, fname, lname);
		rs[0] = getEmployeeById(conn, tableName, id);
		conn.close();
	}

	/**
	 * Insert the record for an employee and return it.
	 *
	 * @param id       ID of employee
	 * @param fname    first name of employee
	 * @param lname    last name of employee
	 * @param rs       employee record that was inserted
	 */
	public static void INSERT_AND_GET_EMPLOYEE_COMMIT_TXN(String tableName, Integer id, String fname, String lname, ResultSet[] rs)
		throws SQLException
	{
		/*
		-- Declare and execute the procedure in ij.
		CREATE PROCEDURE SPLICE.INSERT_AND_GET_EMPLOYEE_COMMIT_TXN(IN tableName VARCHAR(40), IN id INT, IN fname VARCHAR(20), IN lname VARCHAR(30)) PARAMETER STYLE JAVA MODIFIES SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'org.splicetest.txn.TxnTestProcs.INSERT_AND_GET_EMPLOYEE_COMMIT_TXN';
		CALL SPLICE.INSERT_AND_GET_EMPLOYEE_COMMIT_TXN('EMPLOYEE', 2, 'Barney', 'Rubble');
		 */
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		conn.setAutoCommit(false);
		insertEmployee(conn, tableName, id, fname, lname);
		rs[0] = getEmployeeById(conn, tableName, id);
		conn.commit();
		conn.close();
	}

	/**
	 * Insert the record for an employee and return it.
	 *
	 * @param id       ID of employee
	 * @param fname    first name of employee
	 * @param lname    last name of employee
	 * @param rs       employee record that was inserted
	 */
	public static void INSERT_AND_GET_EMPLOYEE_NO_COMMIT_TXN(String tableName, Integer id, String fname, String lname, ResultSet[] rs)
		throws SQLException
	{
		/*
		-- Declare and execute the procedure in ij.
		CREATE PROCEDURE SPLICE.INSERT_AND_GET_EMPLOYEE_NO_COMMIT_TXN(IN tableName VARCHAR(40), IN id INT, IN fname VARCHAR(20), IN lname VARCHAR(30)) PARAMETER STYLE JAVA MODIFIES SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'org.splicetest.txn.TxnTestProcs.INSERT_AND_GET_EMPLOYEE_NO_COMMIT_TXN';
		CALL SPLICE.INSERT_AND_GET_EMPLOYEE_NO_COMMIT_TXN('EMPLOYEE', 2, 'Barney', 'Rubble');
		 */
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		conn.setAutoCommit(false);
		insertEmployee(conn, tableName, id, fname, lname);
		rs[0] = getEmployeeById(conn, tableName, id);
		conn.close();
	}

	/**
	 * Insert the record for an employee and return it.
	 *
	 * @param id       ID of employee
	 * @param fname    first name of employee
	 * @param lname    last name of employee
	 * @param rs       employee record that was inserted
	 */
	public static void INSERT_AND_GET_EMPLOYEE_ROLLBACK_TXN(String tableName, Integer id, String fname, String lname, ResultSet[] rs)
		throws SQLException
	{
		/*
		-- Declare and execute the procedure in ij.
		CREATE PROCEDURE SPLICE.INSERT_AND_GET_EMPLOYEE_ROLLBACK_TXN(IN tableName VARCHAR(40), IN id INT, IN fname VARCHAR(20), IN lname VARCHAR(30)) PARAMETER STYLE JAVA MODIFIES SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'org.splicetest.txn.TxnTestProcs.INSERT_AND_GET_EMPLOYEE_ROLLBACK_TXN';
		CALL SPLICE.INSERT_AND_GET_EMPLOYEE_ROLLBACK_TXN('EMPLOYEE', 2, 'Barney', 'Rubble');
		 */
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		conn.setAutoCommit(false);
		insertEmployee(conn, tableName, id, fname, lname);
		conn.rollback();
		rs[0] = getEmployeeById(conn, tableName, id);
		conn.close();
	}

	/**
	 * Insert the record for an employee and return it.
	 *
	 * @param id       ID of employee
	 * @param fname    first name of employee
	 * @param lname    last name of employee
	 * @param rs       employee record that was inserted
	 */
	public static void INSERT_AND_GET_EMPLOYEE_RELEASE_SVPT(String tableName, Integer id, String fname, String lname, ResultSet[] rs)
		throws SQLException
	{
		/*
		-- Declare and execute the procedure in ij.
		CREATE PROCEDURE SPLICE.INSERT_AND_GET_EMPLOYEE_RELEASE_SVPT(IN tableName VARCHAR(40), IN id INT, IN fname VARCHAR(20), IN lname VARCHAR(30)) PARAMETER STYLE JAVA MODIFIES SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'org.splicetest.txn.TxnTestProcs.INSERT_AND_GET_EMPLOYEE_RELEASE_SVPT';
		CALL SPLICE.INSERT_AND_GET_EMPLOYEE_RELEASE_SVPT('EMPLOYEE', 2, 'Barney', 'Rubble');
		 */
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		conn.setAutoCommit(false);
		Savepoint savepoint0 = conn.setSavepoint("SVPT0");
		insertEmployee(conn, tableName, id, fname, lname);
		rs[0] = getEmployeeById(conn, tableName, id);
		conn.releaseSavepoint(savepoint0);
		conn.close();
	}

	/**
	 * Insert the record for an employee and return it.
	 *
	 * @param id       ID of employee
	 * @param fname    first name of employee
	 * @param lname    last name of employee
	 * @param rs       employee record that was inserted
	 */
	public static void INSERT_AND_GET_EMPLOYEE_NO_RELEASE_SVPT(String tableName, Integer id, String fname, String lname, ResultSet[] rs)
		throws SQLException
	{
		/*
		-- Declare and execute the procedure in ij.
		CREATE PROCEDURE SPLICE.INSERT_AND_GET_EMPLOYEE_NO_RELEASE_SVPT(IN tableName VARCHAR(40), IN id INT, IN fname VARCHAR(20), IN lname VARCHAR(30)) PARAMETER STYLE JAVA MODIFIES SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'org.splicetest.txn.TxnTestProcs.INSERT_AND_GET_EMPLOYEE_NO_RELEASE_SVPT';
		CALL SPLICE.INSERT_AND_GET_EMPLOYEE_NO_RELEASE_SVPT('EMPLOYEE', 2, 'Barney', 'Rubble');
		 */
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		conn.setAutoCommit(false);
		Savepoint savepoint0 = conn.setSavepoint("SVPT0");
		insertEmployee(conn, tableName, id, fname, lname);
		rs[0] = getEmployeeById(conn, tableName, id);
		conn.releaseSavepoint(savepoint0);
		conn.close();
	}

	/**
	 * Insert the record for an employee and return it.
	 *
	 * @param id       ID of employee
	 * @param fname    first name of employee
	 * @param lname    last name of employee
	 * @param rs       employee record that was inserted
	 */
	public static void INSERT_AND_GET_EMPLOYEE_ROLLBACK_SVPT(String tableName, Integer id, String fname, String lname, ResultSet[] rs)
		throws SQLException
	{
		/*
		-- Declare and execute the procedure in ij.
		CREATE PROCEDURE SPLICE.INSERT_AND_GET_EMPLOYEE_ROLLBACK_SVPT(IN tableName VARCHAR(40), IN id INT, IN fname VARCHAR(20), IN lname VARCHAR(30)) PARAMETER STYLE JAVA MODIFIES SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'org.splicetest.txn.TxnTestProcs.INSERT_AND_GET_EMPLOYEE_ROLLBACK_SVPT';
		CALL SPLICE.INSERT_AND_GET_EMPLOYEE_ROLLBACK_SVPT('EMPLOYEE', 2, 'Barney', 'Rubble');
		 */
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		conn.setAutoCommit(false);
		Savepoint savepoint0 = conn.setSavepoint("SVPT0");
		insertEmployee(conn, tableName, id, fname, lname);
		conn.rollback(savepoint0);
		rs[0] = getEmployeeById(conn, tableName, id);
		conn.close();
	}

    /**
     * Searches for an employee by id and returns it, along with two OUTPUT parameters
     * for error code and error message.
     * @param tableName name of employee table
     * @param id id of employee
     * @param errorCode output parameter for error code
     * @param errorMessage output parameter for error message
     * @param rs result set
     * @throws SQLException
     */
    public static void GET_EMPLOYEE_MULTIPLE_OUTPUT_PARAMS(
        String tableName /* IN parameter */,
        Integer id /* IN parameter */,
        String[] errorCode /* OUT parameter */,
        String[] errorMessage, /* OUT parameter */
        ResultSet[] rs) throws SQLException {
    	/*
    	 -- Declare and executet the procedure in ij.
    	 CREATE PROCEDURE SPLICE.GET_EMPLOYEE_MULTIPLE_OUTPUT_PARAMS(IN tableName VARCHAR(40), IN id INT, OUT errorCode VARCHAR(100), OUT errorMessage VARCHAR(100)) PARAMETER STYLE JAVA MODIFIES SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 1 EXTERNAL NAME 'org.splicetest.txn.TxnTestProcs.GET_EMPLOYEE_MULTIPLE_OUTPUT_PARAMS'
    	 */

        String responseErrorCode = "0";
        String responseErrorMessage = "Success";

        Connection conn = DriverManager.getConnection("jdbc:default:connection");
        try {
            rs[0] = getEmployeeById(conn, tableName, id);
        } catch (Exception e) {
            responseErrorCode = "1";
            responseErrorMessage = "Failure - exception fetching record";
        } finally {
            errorCode[0] = responseErrorCode;
            errorMessage[0] = responseErrorMessage;
        }
    }

    // Added test for testing multiple ResultSets
	public static void INSERT_UPDATE_GETx2_DELETE_AND_GET_EMPLOYEE(Integer id,
																   String tableName,
																   String fname,
																   String lname,
																   String fname2,
																   String lname2,
																   Integer id2,
																   ResultSet[] rs1,
																   ResultSet[] rs2,
																   ResultSet[] rs3)
			throws SQLException
	{
		/*
		-- Declare and execute the procedure in ij.
		CREATE PROCEDURE SPLICE.INSERT_UPDATE_GETx2_DELETE_AND_GET_EMPLOYEE(IN id INT, IN tableName VARCHAR(40), IN fname VARCHAR(20), IN lname VARCHAR(30), IN fname2 VARCHAR(20), IN lname2 VARCHAR(30), IN id2 INT) PARAMETER STYLE JAVA MODIFIES SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 3 EXTERNAL NAME 'org.splicetest.sqlj.SqlJTestProcs.INSERT_UPDATE_GETx2_DELETE_AND_GET_EMPLOYEE';
		CALL SPLICE.INSERT_UPDATE_GETx2_DELETE_AND_GET_EMPLOYEE('EMPLOYEE', 2, 'Barney', 'Rubble', 'Wilma', 'Flintsone', 3);
		 */
		Connection conn = DriverManager.getConnection("jdbc:default:connection");
		insertEmployee(conn, tableName, id, fname, lname);
		updateEmployeeNameById(conn, tableName, id, fname2, lname2);
		rs1[0] = getEmployeeFirstNames(conn, tableName);
		rs2[0] = getEmployeeLastNames(conn, tableName);
		deleteEmployee(conn, tableName, id2);
		rs3[0] = getEmployeeById(conn, tableName, id);
		conn.close();
	}

	/*
	 * ============================================================================================
	 * Worker methods
	 * ============================================================================================
	 */

	/**
	 * Worker method that creates the EMPLOYEE table.
	 * @param conn connection
	 * @param tableName  name of EMPLOYEE table
	 * @throws SQLException
	 */
	private static void createEmployeeTable(Connection conn, String tableName) throws SQLException {
		Statement stmt = conn.createStatement();
		stmt.executeUpdate("create table " + tableName + " (ID int, FNAME varchar(20), LNAME varchar(30))");
	}

	/**
	 * Worker method that drops the EMPLOYEE table.
	 * @param conn connection
	 * @param tableName  name of EMPLOYEE table
	 * @throws SQLException
	 */
	private static void dropEmployeeTable(Connection conn, String tableName) throws SQLException {
		Statement stmt = conn.createStatement();
		stmt.executeUpdate("drop table " + tableName);
	}

	/**
	 * Worker method that inserts an employee into the EMPLOYEE table.
	 * @param conn connection
	 * @param tableName  name of EMPLOYEE table
	 * @param id       ID of employee
	 * @param fname    first name of employee
	 * @param lname    last name of employee
	 * @throws SQLException
	 */
	private static void insertEmployee(Connection conn, String tableName, Integer id, String fname, String lname) throws SQLException {
		PreparedStatement pstmt = conn.prepareStatement("insert into " + tableName + " values(?, ?, ?)");
		pstmt.setInt(1, id);
		pstmt.setString(2, fname);
		pstmt.setString(3, lname);
		pstmt.executeUpdate();
	}

	/**
	 * Worker method that updates the name of an employee in the EMPLOYEE table by ID.
	 * @param conn connection
	 * @param tableName  name of EMPLOYEE table
	 * @param id       ID of employee
	 * @param fname    first name of employee
	 * @param lname    last name of employee
	 * @throws SQLException
	 */
	private static void updateEmployeeNameById(Connection conn, String tableName, Integer id, String fname, String lname) throws SQLException {
		PreparedStatement pstmt = conn.prepareStatement("update " + tableName + " set FNAME = ?, LNAME = ? where ID = ?");
		pstmt.setString(1, fname);
		pstmt.setString(2, lname);
		pstmt.setInt(3, id);
		pstmt.executeUpdate();
	}

	/**
	 * Worker method that deletes an employee from the EMPLOYEE table.
	 * @param conn connection
	 * @param tableName  name of EMPLOYEE table
	 * @param id       ID of employee
	 * @throws SQLException
	 */
	private static void deleteEmployee(Connection conn, String tableName, Integer id) throws SQLException {
		PreparedStatement pstmt = conn.prepareStatement("delete from " + tableName + " where ID = ?");
		pstmt.setInt(1, id);
		pstmt.executeUpdate();
	}

	/**
	 * Worker method that returns an employee by ID.
	 * @param conn connection
	 * @param tableName  name of EMPLOYEE table
	 * @param id       ID of employee
	 * @throws SQLException
	 */
	private static ResultSet getEmployeeById(Connection conn, String tableName, Integer id) throws SQLException {
		PreparedStatement pstmt = conn.prepareStatement("select * from " + tableName + " where ID = ?");
		pstmt.setInt(1, id);
		return pstmt.executeQuery();
	}

	/**
	 * Worker method that returns all employee first names.
	 * @param conn connection
	 * @param tableName  name of EMPLOYEE table
	 * @throws SQLException
	 */
	private static ResultSet getEmployeeFirstNames(Connection conn, String tableName) throws SQLException {
		PreparedStatement pstmt = conn.prepareStatement("select FNAME from " + tableName);
		return pstmt.executeQuery();
	}

	/**
	 * Worker method that returns all employee last names.
	 * @param conn connection
	 * @param tableName  name of EMPLOYEE table
	 * @throws SQLException
	 */
	private static ResultSet getEmployeeLastNames(Connection conn, String tableName) throws SQLException {
		PreparedStatement pstmt = conn.prepareStatement("select LNAME from " + tableName);
		return pstmt.executeQuery();
	}

	public static void MULTIPLE_RESULTSETS_PROC(ResultSet[] rs1, ResultSet[] rs2, ResultSet[] rs3)
			throws SQLException
	{
		//-- Declare and execute the procedure in ij.
		//CREATE PROCEDURE SPLICE.MULTIPLE_RESULTSETS_PROC() PARAMETER STYLE JAVA READS SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 3 EXTERNAL NAME 'org.splicetest.txn.TxnTestProcs.MULTIPLE_RESULTSETS_PROC';
		//CALL SPLICE.MULTIPLE_RESULTSETS_PROC();
		Connection c = DriverManager.getConnection("jdbc:default:connection");
		Statement stmt = c.createStatement();
		rs1[0] = c.createStatement().executeQuery("VALUES(1)");
		rs2[0] = c.createStatement().executeQuery("VALUES(2)");
		rs3[0] = c.createStatement().executeQuery("VALUES(3)");
		c.close();

	}

	/*
	 * @param out output parameter which is an integer
	 *
	 */
	public static void OUTPUT_PARAMETER_NO_RESULTSET(Integer[] out)
		throws SQLException{
		//-- Declare and execute the procedure in ij.
		//CREATE PROCEDURE SPLICE.OUTPUT_PARAMETER_NO_RESULTSET(OUT outInt INT) PARAMETER STYLE JAVA READS SQL DATA LANGUAGE JAVA DYNAMIC RESULT SETS 0 EXTERNAL NAME 'org.splicetest.txn.TxnTestProcs.OUTPUT_PARAMETER_NO_RESULTSET';
		out[0] = 1;
		return;
	}
}
