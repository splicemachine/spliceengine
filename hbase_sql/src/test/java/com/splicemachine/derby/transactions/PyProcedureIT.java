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

package com.splicemachine.derby.transactions;

import java.sql.*;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test.SerialTest;
import com.splicemachine.test.Transactions;

/**
 * This class is the IT test for Python Stored Procedure. It has two parts.
 * The first part mimics the CallableTransactionIT test under the hbase_sql project, testing the transactional correctness.
 * However, tests for savepoint are not included as zxJDBC does not support savepoint as far as I know.
 * SQLAlchemy might be the tool for enabling savepoint, but even this only supports certain DB.
 * The second part mimics the ProcedureTest test under the db-testing, focusing on whether the Python Stored Procedure
 * behaves properly when dealing with different number of ResultSets, IN/OUT parameters, and the correctness of the
 * PyStoredProcedureResultSetFacotry.
 *
 * @author Ao Zeng
 *		 Created on: 6/25/2018
 */
@Category({Transactions.class,SerialTest.class}) //made serial because it loads a jar
public class PyProcedureIT extends SpliceUnitTest {

    public static final String CLASS_NAME = PyProcedureIT.class.getSimpleName().toUpperCase();

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    // Names of files and SQL objects.
    private static final String SCHEMA_NAME = CLASS_NAME;
    private static final String EMPLOYEE_TABLE_NAME_BASE = SCHEMA_NAME + ".EMPLOYEE";

    // SQL statements to create and drop stored procedures.
    private static final String CREATE_PROC_CREATE_EMPLOYEE_TABLE = String.format("CREATE PROCEDURE %s.CREATE_EMPLOYEE_TABLE(IN tableName VARCHAR(40)) PARAMETER STYLE JAVA MODIFIES SQL DATA LANGUAGE PYTHON DYNAMIC RESULT SETS 0 AS 'def run(tableName):\n" +
            "    c = conn.cursor()\n" +
            "    c.execute(\"create table \" + tableName + \" (ID int, FNAME varchar(20), LNAME varchar(30))\")\n" +
            "    conn.commit()\n" +
            "    c.close()\n" +
            "    conn.close()'", SCHEMA_NAME);
    private static final String DROP_PROC_CREATE_EMPLOYEE_TABLE = String.format("DROP PROCEDURE %s.CREATE_EMPLOYEE_TABLE", SCHEMA_NAME);

    private static final String CREATE_PROC_DROP_EMPLOYEE_TABLE = String.format("CREATE PROCEDURE %s.DROP_EMPLOYEE_TABLE(IN tableName VARCHAR(40)) PARAMETER STYLE JAVA MODIFIES SQL DATA LANGUAGE PYTHON DYNAMIC RESULT SETS 0 AS 'def run(tableName):\n" +
            "    c = conn.cursor()\n" +
            "    c.execute(\"drop table \" + tableName)\n" +
            "    conn.commit()\n" +
            "    c.close()\n" +
            "    conn.close()'", SCHEMA_NAME);
    private static final String DROP_PROC_DROP_EMPLOYEE_TABLE = String.format("DROP PROCEDURE %s.DROP_EMPLOYEE_TABLE", SCHEMA_NAME);

    private static final String CREATE_PROC_INSERT_EMPLOYEE = String.format("CREATE PROCEDURE %s.INSERT_EMPLOYEE(IN tableName VARCHAR(40), IN id INT, IN fname VARCHAR(20), IN lname VARCHAR(30)) PARAMETER STYLE JAVA MODIFIES SQL DATA LANGUAGE PYTHON DYNAMIC RESULT SETS 0 AS 'def run(tableName, empId, fname, lname):\n" +
            "    c = conn.cursor()\n" +
            "    stmt = \"insert into \" + tableName + \" values(?, ?, ?)\"\n" +
            "    c.execute(stmt, [empId, fname, lname])\n" +
            "    conn.commit()\n" +
            "    c.close()\n" +
            "    conn.close()'", SCHEMA_NAME);
    private static final String DROP_PROC_INSERT_EMPLOYEE = String.format("DROP PROCEDURE %s.INSERT_EMPLOYEE", SCHEMA_NAME);

    private static final String CREATE_PROC_GET_EMPLOYEE = String.format("CREATE PROCEDURE %s.GET_EMPLOYEE(IN tableName VARCHAR(40), IN id INT) PARAMETER STYLE JAVA READS SQL DATA LANGUAGE PYTHON DYNAMIC RESULT SETS 1 AS 'def run(tableName, empId, rs):\n" +
            "    c = conn.cursor()\n" +
            "    stmt = \"select * from \" + tableName + \" where ID = ?\"\n" +
            "    c.execute(stmt,[empId])\n" +
            "    d = c.description\n" +
            "    result = c.fetchall()\n" +
            "    rs[0] = factory.create([d,result])\n" +
            "    conn.commit()\n" +
            "    c.close()\n" +
            "    conn.close()'", SCHEMA_NAME);
    private static final String DROP_PROC_GET_EMPLOYEE = String.format("DROP PROCEDURE %s.GET_EMPLOYEE", SCHEMA_NAME);

    private static final String CREATE_PROC_INSERT_AND_GET_EMPLOYEE = String.format("CREATE PROCEDURE %s.INSERT_AND_GET_EMPLOYEE(IN tableName VARCHAR(40), IN id INT, IN fname VARCHAR(20), IN lname VARCHAR(30)) PARAMETER STYLE JAVA MODIFIES SQL DATA LANGUAGE PYTHON DYNAMIC RESULT SETS 1 AS 'def run(tableName, empId, fname, lname, rs):\n" +
            "    c = conn.cursor()\n" +
            "    insertStmt = \"insert into \" + tableName + \" values(?, ?, ?)\"\n" +
            "    c.execute(insertStmt,[empId, fname, lname])\n" +
            "    getStmt = \"select * from \" + tableName + \" where ID = ?\"\n" +
            "    c.execute(getStmt,[empId])\n" +
            "    d = c.description\n" +
            "    result = c.fetchall()\n" +
            "    rs[0] = factory.create([d,result])\n" +
            "    conn.commit()\n" +
            "    c.close()\n" +
            "    conn.close()'", SCHEMA_NAME);
    private static final String DROP_PROC_INSERT_AND_GET_EMPLOYEE = String.format("DROP PROCEDURE %s.INSERT_AND_GET_EMPLOYEE", SCHEMA_NAME);

    private static final String CREATE_PROC_CREATE_INSERT_AND_GET_EMPLOYEE = String.format("CREATE PROCEDURE %s.CREATE_INSERT_AND_GET_EMPLOYEE(IN tableName VARCHAR(40), IN id INT, IN fname VARCHAR(20), IN lname VARCHAR(30)) PARAMETER STYLE JAVA MODIFIES SQL DATA LANGUAGE PYTHON DYNAMIC RESULT SETS 1 AS 'def run(tableName, empId, fname, lname, rs):\n" +
            "    c = conn.cursor()\n" +
            "    createStmt = \"create table \" + tableName + \" (ID int, FNAME varchar(20), LNAME varchar(30))\"\n" +
            "    c.execute(createStmt)\n" +
            "\n" +
            "    insertStmt = \"insert into \" + tableName + \" values(?, ?, ?)\"\n" +
            "    c.execute(insertStmt ,[empId, fname, lname])\n" +
            "\n" +
            "    getStmt = \"select * from \" + tableName + \" where ID = ?\"\n" +
            "    c.execute(getStmt,[empId])\n" +
            "\n" +
            "    d = c.description\n" +
            "    result = c.fetchall()\n" +
            "    rs[0] = factory.create([d,result])\n" +
            "    conn.commit()\n" +
            "    c.close()\n" +
            "    conn.close()'", SCHEMA_NAME);
    private static final String DROP_PROC_CREATE_INSERT_AND_GET_EMPLOYEE = String.format("DROP PROCEDURE %s.CREATE_INSERT_AND_GET_EMPLOYEE", SCHEMA_NAME);

    // For zxJDBC autocommit is turned off by default. Also, in nested conneciton, the auto commit cannot be turned on.
    private static final String CREATE_PROC_INSERT_AND_GET_EMPLOYEE_NO_COMMIT_TXN = String.format("CREATE PROCEDURE %s.INSERT_AND_GET_EMPLOYEE_NO_COMMIT_TXN(IN tableName VARCHAR(40), IN id INT, IN fname VARCHAR(20), IN lname VARCHAR(30)) PARAMETER STYLE JAVA MODIFIES SQL DATA LANGUAGE PYTHON DYNAMIC RESULT SETS 1 AS 'def run(tableName, empId, fname, lname, rs):\n" +
            "    c = conn.cursor()\n" +
            "    insertStmt = \"insert into \" + tableName + \" values(?, ?, ?)\"\n" +
            "    c.execute(insertStmt, [empId, fname, lname])\n" +
            "\n" +
            "    getStmt = \"select * from \" + tableName + \" where ID = ?\"\n" +
            "    c.execute(getStmt,[empId])\n" +
            "\n" +
            "    d = c.description\n" +
            "    result = c.fetchall()\n" +
            "    rs[0] = factory.create([d,result])\n" +
            "    c.close()\n" +
            "    conn.close()'", SCHEMA_NAME);
    private static final String DROP_PROC_INSERT_AND_GET_EMPLOYEE_NO_COMMIT_TXN = String.format("DROP PROCEDURE %s.INSERT_AND_GET_EMPLOYEE_NO_COMMIT_TXN", SCHEMA_NAME);

    private static final String CREATE_PROC_INSERT_AND_GET_EMPLOYEE_ROLLBACK_TXN = String.format("CREATE PROCEDURE %s.INSERT_AND_GET_EMPLOYEE_ROLLBACK_TXN(IN tableName VARCHAR(40), IN id INT, IN fname VARCHAR(20), IN lname VARCHAR(30)) PARAMETER STYLE JAVA MODIFIES SQL DATA LANGUAGE PYTHON DYNAMIC RESULT SETS 1 AS 'def run(tableName, empId, fname, lname, rs):\n" +
            "    c = conn.cursor()\n" +
            "    insertStmt = \"insert into \" + tableName + \" values(?, ?, ?)\"\n" +
            "    c.execute(insertStmt, [empId, fname, lname])\n" +
            "\n" +
            "    conn.rollback()\n" +
            "\n" +
            "    getStmt = \"select * from \" + tableName + \" where ID = ?\"\n" +
            "    c.execute(getStmt,[empId])\n" +
            "\n" +
            "    d = c.description\n" +
            "    result = c.fetchall()\n" +
            "    rs[0] = factory.create([d,result])\n" +
            "    c.close()\n" +
            "    conn.close()'", SCHEMA_NAME);
    private static final String DROP_PROC_INSERT_AND_GET_EMPLOYEE_ROLLBACK_TXN = String.format("DROP PROCEDURE %s.INSERT_AND_GET_EMPLOYEE_ROLLBACK_TXN", SCHEMA_NAME);

    private static final String CREATE_PROC_GET_EMPLOYEE_MULTIPLE_OUTPUT_PARAMS = String.format("CREATE PROCEDURE %s.GET_EMPLOYEE_MULTIPLE_OUTPUT_PARAMS(IN tableName VARCHAR(40), IN id INT, OUT errorCode VARCHAR(100), OUT errorMessage VARCHAR(100)) " +
            "PARAMETER STYLE JAVA MODIFIES SQL DATA LANGUAGE PYTHON DYNAMIC RESULT SETS 1 AS 'def run(tableName, empId, customizedMessage1, customizedMessage2, rs):\n" +
            "    c = conn.cursor()\n" +
            "    customizedMessage1[0] = \"The first message\"\n" +
            "    customizedMessage2[0] = \"The second message\"\n" +
            "    stmt = \"select * from \" + tableName + \" where ID = ?\"\n" +
            "    c.execute(stmt,[empId])\n" +
            "    d = c.description\n" +
            "    result = c.fetchall()\n" +
            "    rs[0] = factory.create([d,result])\n" +
            "    conn.commit()\n" +
            "    conn.close()\n" +
            "    return'", SCHEMA_NAME);
    private static final String DROP_PROC_GET_EMPLOYEE_MULTIPLE_OUTPUT_PARAMS = String.format("DROP PROCEDURE %s.GET_EMPLOYEE_MULTIPLE_OUTPUT_PARAMS", SCHEMA_NAME);

    // SQL statements to call stored procedures.
    private static final String CALL_CREATE_EMPLOYEE_TABLE_FORMAT_STRING = "CALL " + SCHEMA_NAME + ".CREATE_EMPLOYEE_TABLE('%s')";
    private static final String CALL_DROP_EMPLOYEE_TABLE_FORMAT_STRING = "CALL " + SCHEMA_NAME + ".DROP_EMPLOYEE_TABLE('%s')";
    private static final String CALL_INSERT_EMPLOYEE_FORMAT_STRING = "CALL " + SCHEMA_NAME + ".INSERT_EMPLOYEE('%s', 2, 'Barney', 'Rubble')";
    private static final String CALL_GET_EMPLOYEE_FORMAT_STRING = "CALL " + SCHEMA_NAME + ".GET_EMPLOYEE('%s', 2)";
    private static final String CALL_INSERT_AND_GET_EMPLOYEE_FORMAT_STRING = "CALL " + SCHEMA_NAME + ".INSERT_AND_GET_EMPLOYEE('%s', 2, 'Barney', 'Rubble')";
    private static final String CALL_CREATE_INSERT_AND_GET_EMPLOYEE_FORMAT_STRING = "CALL " + SCHEMA_NAME + ".CREATE_INSERT_AND_GET_EMPLOYEE('%s', 1, 'Fred', 'Flintstone')";
    private static final String CALL_INSERT_AND_GET_EMPLOYEE_NO_COMMIT_TXN_FORMAT_STRING = "CALL " + SCHEMA_NAME + ".INSERT_AND_GET_EMPLOYEE_NO_COMMIT_TXN('%s', 2, 'Barney', 'Rubble')";
    private static final String CALL_INSERT_AND_GET_EMPLOYEE_ROLLBACK_TXN_FORMAT_STRING = "CALL " + SCHEMA_NAME + ".INSERT_AND_GET_EMPLOYEE_ROLLBACK_TXN('%s', 2, 'Barney', 'Rubble')";
    private static final String CALL_GET_EMPLOYEE_MULTIPLE_OUTPUT_PARAMS = "CALL " + SCHEMA_NAME + ".GET_EMPLOYEE_MULTIPLE_OUTPUT_PARAMS(?, ?, ?, ?)";

    // SQL queries.
    private static final String SELECT_FROM_SYSTABLES_BY_TABLENAME = "SELECT * FROM SYS.SYSTABLES WHERE TABLENAME = ?";

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @BeforeClass
    public static void setUpClass() throws Exception {
        // Create the user-defined stored procedures to create and drop the EMPLOYEE table.
        spliceClassWatcher.executeUpdate(CREATE_PROC_CREATE_EMPLOYEE_TABLE);
        spliceClassWatcher.executeUpdate(CREATE_PROC_DROP_EMPLOYEE_TABLE);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {

        // Drop the user-defined stored procedures to create and drop the EMPLOYEE table.
        spliceClassWatcher.executeUpdate(DROP_PROC_CREATE_EMPLOYEE_TABLE);
        spliceClassWatcher.executeUpdate(DROP_PROC_DROP_EMPLOYEE_TABLE);
    }

    /*
     * ============================================================================================
     * Test cases for Transactional Correctness
     * ============================================================================================
     */

    /**
     * Test creating a table and dropping a table in two different stored procedures.
     * @throws Exception
     */
    @Test
    public void testCreateTableDropTable2Procedures() throws Exception {
        String employeeTableName = EMPLOYEE_TABLE_NAME_BASE + "1";
        int rc = 0;

        // Create the table.
        rc = methodWatcher.executeUpdate(String.format(CALL_CREATE_EMPLOYEE_TABLE_FORMAT_STRING, employeeTableName));
        Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

        // Drop the table.
        rc = methodWatcher.executeUpdate(String.format(CALL_DROP_EMPLOYEE_TABLE_FORMAT_STRING, employeeTableName));
        Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);
    }

    /**
     * Test creating a table, inserting a row, returning a scanned row, and dropping the table in four different stored procedures.
     * @throws Exception
     */
    @Test
    public void testCreateTableInsertRowSelectRowDropTable4Procedures() throws Exception {
        String employeeTableName = EMPLOYEE_TABLE_NAME_BASE + "2";
        int rc = 0;
        ResultSet rs = null;

        // Create the user-defined stored procedures.
        rc = methodWatcher.executeUpdate(CREATE_PROC_INSERT_EMPLOYEE);
        Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);
        rc = methodWatcher.executeUpdate(CREATE_PROC_GET_EMPLOYEE);
        Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

        // Create the table.
        rc = methodWatcher.executeUpdate(String.format(CALL_CREATE_EMPLOYEE_TABLE_FORMAT_STRING, employeeTableName));
        Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

        // Insert the row.
        rc = methodWatcher.executeUpdate(String.format(CALL_INSERT_EMPLOYEE_FORMAT_STRING, employeeTableName));
        Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

        // Get the row.
        rs = methodWatcher.executeQuery(String.format(CALL_GET_EMPLOYEE_FORMAT_STRING, employeeTableName));
        Assert.assertEquals("Incorrect # of rows returned!", 1, resultSetSize(rs));
        rs.close();

        // Drop the user-defined stored procedures.
        rc = methodWatcher.executeUpdate(DROP_PROC_INSERT_EMPLOYEE);
        Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);
        rc = methodWatcher.executeUpdate(DROP_PROC_GET_EMPLOYEE);
        Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

        // Drop the table.
        rc = methodWatcher.executeUpdate(String.format(CALL_DROP_EMPLOYEE_TABLE_FORMAT_STRING, employeeTableName));
        Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);
    }

    /**
     * Test inserting a row and returning a scanned row in one stored procedure.
     * @throws Exception
     */
    @Test
    public void testCreateTableInsertAndSelectRowDropTable3Procedures() throws Exception {
        String employeeTableName = EMPLOYEE_TABLE_NAME_BASE + "3";
        int rc = 0;
        ResultSet rs = null;

        // Create the user-defined stored procedures.
        rc = methodWatcher.executeUpdate(CREATE_PROC_INSERT_AND_GET_EMPLOYEE);
        Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

        // Create the table.
        rc = methodWatcher.executeUpdate(String.format(CALL_CREATE_EMPLOYEE_TABLE_FORMAT_STRING, employeeTableName));
        Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

        // Insert and get the row in one stored procedure.
        rs = methodWatcher.executeQuery(String.format(CALL_INSERT_AND_GET_EMPLOYEE_FORMAT_STRING, employeeTableName));
        Assert.assertEquals("Incorrect # of rows returned!", 1, resultSetSize(rs));
        rs.close();

        // Drop the user-defined stored procedures.
        rc = methodWatcher.executeUpdate(DROP_PROC_INSERT_AND_GET_EMPLOYEE);
        Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

        // Drop the table.
        rc = methodWatcher.executeUpdate(String.format(CALL_DROP_EMPLOYEE_TABLE_FORMAT_STRING, employeeTableName));
        Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);
    }

    /**
     * Test creating a table, inserting a row, and returning a scanned row all in one stored procedure.
     * @throws Exception
     */
    @Test
    public void testCreateTableInsertAndSelectRowDropTable2Procedures() throws Exception {
        String employeeTableName = EMPLOYEE_TABLE_NAME_BASE + "4";
        int rc = 0;
        ResultSet rs = null;

        // Create the user-defined stored procedures.
        rc = methodWatcher.executeUpdate(CREATE_PROC_CREATE_INSERT_AND_GET_EMPLOYEE);
        Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

        // Create the table, insert a row, and get the row in one stored procedure.
        rs = methodWatcher.executeQuery(String.format(CALL_CREATE_INSERT_AND_GET_EMPLOYEE_FORMAT_STRING, employeeTableName));
        Assert.assertEquals("Incorrect # of rows returned!", 1, resultSetSize(rs));
        rs.close();

        // Drop the user-defined stored procedures.
        rc = methodWatcher.executeUpdate(DROP_PROC_CREATE_INSERT_AND_GET_EMPLOYEE);
        Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

        // Drop the table.
        rc = methodWatcher.executeUpdate(String.format(CALL_DROP_EMPLOYEE_TABLE_FORMAT_STRING, employeeTableName));
        Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);
    }

    /**
     * Test inserting a row and returning a scanned row in one stored procedure without an explicit transaction commit.
     * @throws Exception
     */
    @Test
    public void testInsertAndSelectRowProcedureWithoutExplicitTransactionCommit() throws Exception {
        String employeeTableName = EMPLOYEE_TABLE_NAME_BASE + "6";
        int rc = 0;
        ResultSet rs = null;

        // Create the user-defined stored procedures.
        rc = methodWatcher.executeUpdate(CREATE_PROC_INSERT_AND_GET_EMPLOYEE_NO_COMMIT_TXN);
        Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

        // Create the table.
        rc = methodWatcher.executeUpdate(String.format(CALL_CREATE_EMPLOYEE_TABLE_FORMAT_STRING, employeeTableName));
        Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

        // Insert and get the row in one stored procedure.
        rs = methodWatcher.executeQuery(String.format(CALL_INSERT_AND_GET_EMPLOYEE_NO_COMMIT_TXN_FORMAT_STRING, employeeTableName));
        Assert.assertEquals("Incorrect # of rows returned!", 1, resultSetSize(rs));
        rs.close();

        // Drop the user-defined stored procedures.
        rc = methodWatcher.executeUpdate(DROP_PROC_INSERT_AND_GET_EMPLOYEE_NO_COMMIT_TXN);
        Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

        // Drop the table.
        rc = methodWatcher.executeUpdate(String.format(CALL_DROP_EMPLOYEE_TABLE_FORMAT_STRING, employeeTableName));
        Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);
    }

    /**
     * Test inserting a row and returning a scanned row in one stored procedure with a transaction rollback.
     * @throws Exception
     */
    @Test
    public void testInsertAndSelectRowProcedureWithTransactionRollback() throws Exception {
        String employeeTableName = EMPLOYEE_TABLE_NAME_BASE + "7";
        int rc = 0;
        ResultSet rs = null;

        // Create the user-defined stored procedures.
        rc = methodWatcher.executeUpdate(CREATE_PROC_INSERT_AND_GET_EMPLOYEE_ROLLBACK_TXN);
        Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

        // Create the table.
        rc = methodWatcher.executeUpdate(String.format(CALL_CREATE_EMPLOYEE_TABLE_FORMAT_STRING, employeeTableName));
        Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

        // Insert and get the row in one stored procedure.
        rs = methodWatcher.executeQuery(String.format(CALL_INSERT_AND_GET_EMPLOYEE_ROLLBACK_TXN_FORMAT_STRING, employeeTableName));
        Assert.assertEquals("Incorrect # of rows returned!", 0, resultSetSize(rs));
        rs.close();

        // Drop the user-defined stored procedures.
        rc = methodWatcher.executeUpdate(DROP_PROC_INSERT_AND_GET_EMPLOYEE_ROLLBACK_TXN);
        Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

        // Drop the table.
        rc = methodWatcher.executeUpdate(String.format(CALL_DROP_EMPLOYEE_TABLE_FORMAT_STRING, employeeTableName));
        Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);
    }

    /**
     * Tests ability for stored procedure to return not just a result set,
     * but multiple OUTPUT parameters.
     * @throws Exception
     */
    @Test
    public void testProcedureWithMultipleOutputParameters() throws Exception {
        String employeeTableName = EMPLOYEE_TABLE_NAME_BASE + "11";
        int rc = 0;
        ResultSet rs = null;

        // Create the user-defined stored procedures.
        rc = methodWatcher.executeUpdate(CREATE_PROC_GET_EMPLOYEE_MULTIPLE_OUTPUT_PARAMS);
        Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

        rc = methodWatcher.executeUpdate(CREATE_PROC_INSERT_EMPLOYEE);
        Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

        // Create the table.
        rc = methodWatcher.executeUpdate(String.format(CALL_CREATE_EMPLOYEE_TABLE_FORMAT_STRING, employeeTableName));
        Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

        // Insert into the table.
        rc = methodWatcher.executeUpdate(String.format(CALL_INSERT_EMPLOYEE_FORMAT_STRING, employeeTableName));
        Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

        CallableStatement stmt = methodWatcher.prepareCall(CALL_GET_EMPLOYEE_MULTIPLE_OUTPUT_PARAMS);
        stmt.setString(1, employeeTableName);
        stmt.setLong(2, 2);
        stmt.registerOutParameter(3, Types.VARCHAR);
        stmt.registerOutParameter(4, Types.VARCHAR);

        stmt.execute();
        rs = stmt.getResultSet();

        Assert.assertEquals("Incorrect # of rows returned!", 1, resultSetSize(rs));
        Assert.assertEquals("Incorrect customized message1", "The first message", stmt.getString(3));
        Assert.assertEquals("Incorrect customized message2", "The second message", stmt.getString(4));
        rs.close();

        rc = methodWatcher.executeUpdate(DROP_PROC_INSERT_EMPLOYEE);
        Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

        // Drop the user-defined stored procedures.
        rc = methodWatcher.executeUpdate(DROP_PROC_GET_EMPLOYEE_MULTIPLE_OUTPUT_PARAMS);
        Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);

        // Drop the table.
        rc = methodWatcher.executeUpdate(String.format(CALL_DROP_EMPLOYEE_TABLE_FORMAT_STRING, employeeTableName));
        Assert.assertEquals("Incorrect return code or result count returned!", 0, rc);
    }

    /*
     * ============================================================================================
     * Test cases for Procedure Correctness
     * ============================================================================================
     */
    final static String CREATE_SIMPLE_TABLE = "CREATE TABLE SIMPLE_TABLE (id INT)";
    final static String DROP_SIMPLE_TABLE_IF_EXISTS = "DROP TABLE SIMPLE_TABLE IF EXISTS";

    final static String CREATE_PROC_RETRIEVE_DYNAMIC_RESULTS = String.format("CREATE PROCEDURE %s.RETRIEVE_DYNAMIC_RESULTS(IN number INT) " +
        "PARAMETER STYLE JAVA READS SQL DATA LANGUAGE PYTHON DYNAMIC RESULT SETS 4 AS 'def run(number, rs1, rs2, rs3, rs4):\n" +
            "    if (number > 0):\n" +
            "        c1 = conn.cursor()\n" +
            "        c1.execute(\"VALUES(1)\")\n" +
            "        rs1[0] = factory.create([c1.description, c1.fetchall()])\n" +
            "        c1.close()\n" +
            "    if (number > 1):\n" +
            "        c2 = conn.cursor()\n" +
            "        c2.execute(\"VALUES(2)\")\n" +
            "        rs2[0] = factory.create([c2.description, c2.fetchall()])\n" +
            "        c2.close()\n" +
            "    if (number > 2):\n" +
            "        c3 = conn.cursor()\n" +
            "        c3.execute(\"VALUES(3)\")\n" +
            "        rs3[0] = factory.create([c3.description, c3.fetchall()])\n" +
            "        c3.close()\n" +
            "    if (number > 3):\n" +
            "        c4 = conn.cursor()\n" +
            "        c4.execute(\"VALUES(4)\")\n" +
            "        rs4[0] = factory.create([c4.description, c4.fetchall()])\n" +
            "        c4.close()\n" +
            "    conn.close()' ",SCHEMA_NAME);
    final static String DROP_PROC_RETRIEVE_DYNAMIC_RESULTS = String.format("DROP PROCEDURE %s.RETRIEVE_DYNAMIC_RESULTS", SCHEMA_NAME);

    final static String CREATE_PROC_RETRIEVE_CLOSED_RESULT = String.format("CREATE PROCEDURE %s.RETRIEVE_CLOSED_RESULT() " +
            "LANGUAGE PYTHON DYNAMIC RESULT SETS 1 PARAMETER STYLE JAVA AS 'def run(closedRs):\n" +
            "    c = conn.cursor()\n" +
            "    c.execute(\"VALUES(1)\");\n" +
            "    closedRs = factory.create([c.description,c.fetchall()])\n" +
            "    closedRs.close()\n" +
            "    conn.close()'",SCHEMA_NAME);
    final static String DROP_PROC_RETRIEVE_CLOSED_RESULT = String.format("DROP PROCEDURE %s.RETRIEVE_CLOSED_RESULT", SCHEMA_NAME);

    final static String CREATE_PROC_RETRIEVE_EXTERNAL_RESULT = String.format("CREATE PROCEDURE %s.RETRIEVE_EXTERNAL_RESULT(IN DBNAME VARCHAR(128), IN DBUSER VARCHAR(128), IN DBPWD VARCHAR(128)) " +
            "LANGUAGE PYTHON DYNAMIC RESULT SETS 1 PARAMETER STYLE JAVA AS 'def run(dbName, user, password, externalRs):\n" +
            "    global conn\n" +
            "    conn.close()\n" +  // Close the defualt connection
            "    url = \"jdbc:splice:localhost:1527/\" + dbName;\n" +
            "    conn = zxJDBC.connect(url, user, password, driver)\n" +
            "    c = conn.cursor()\n" +
            "    c.execute(\"VALUES(1)\")\n" +
            "    externalRs[0] = factory.create([c.description,c.fetchall()])\n" +
            "    conn.commit()\n" +
            "    conn.close()'",SCHEMA_NAME);
    final static String DROP_PROC_RETRIEVE_EXTERNAL_RESULT = String.format("DROP PROCEDURE %s.RETRIEVE_EXTERNAL_RESULT", SCHEMA_NAME);

    final static String CREATE_PROC_PRIM_OUT = String.format("CREATE PROCEDURE %s.PRIM_OUT(OUT outByte TINYINT, OUT outShort SMALLINT, OUT outLong BIGINT, OUT outBoolean BOOLEAN, OUT outDouble DOUBLE, OUT outFloat REAL, OUT outInt INTEGER) PARAMETER STYLE JAVA READS SQL DATA LANGUAGE PYTHON DYNAMIC RESULT SETS 0 AS 'def run(outByte, outShort, outLong, outBoolean, outDouble, outFloat, outInt):\n" +
            "    outByte[0] = 1\n" +
            "    outShort[0] = 32767\n" +
            "    outLong[0] = 9223372036854775807\n" +
            "    outBoolean[0] = True\n" +
            "    outDouble[0] = 1.7969E+208\n" +
            "    outFloat[0] = 3.402E+38\n" +
            "    outInt[0] = 2147483647'",SCHEMA_NAME);
    final static String DROP_PROC_PRIM_OUT = String.format("DROP PROCEDURE %s.PRIM_OUT", SCHEMA_NAME);

    final static String CREATE_PROC_INT_OUT = String.format("CREATE PROCEDURE %s.INT_OUT(OUT outInt INT) " +
            "PARAMETER STYLE JAVA READS SQL DATA LANGUAGE PYTHON DYNAMIC RESULT SETS 0 AS 'def run(out):\n" +
            "    out[0] = 42'",SCHEMA_NAME);
    final static String DROP_PROC_INT_OUT = String.format("DROP PROCEDURE %s.INT_OUT", SCHEMA_NAME);

    // test credentials
    final static String TEST_DBNAME = "splicedb";
    final static String TEST_USERNAME = "splice";
    final static String TEST_PASSWORD = "admin";

    /**
     * Tests that <code>Statement.executeQuery()</code> fails when no
     * result sets are returned (even thought the Maximum Dynamic ResultSets is set to be larger than 1).
     * @exception SQLException if a database error occurs
     */
    @Test
    public void testExecuteQueryWithNoDynamicResultSets() throws Exception {
        ResultSet rs;
        try {
            methodWatcher.executeUpdate(CREATE_PROC_RETRIEVE_DYNAMIC_RESULTS);
            rs = methodWatcher.executeQuery(String.format("CALL %s.RETRIEVE_DYNAMIC_RESULTS(0)",SCHEMA_NAME));
            Assert.fail("executeQuery() didn't fail.");
        } catch (SQLException sqle) {
            Assert.assertEquals("X0Y78", sqle.getSQLState());
        }
        methodWatcher.executeUpdate(DROP_SIMPLE_TABLE_IF_EXISTS);
        methodWatcher.executeUpdate(DROP_PROC_RETRIEVE_DYNAMIC_RESULTS);
    }

    /**
     * Tests that <code>Statement.executeQuery()</code> succeeds when
     * one result set is returned from a stored procedure.
     * @exception SQLException if a database error occurs
     */
    @Test
    public void testExecuteQueryWithOneDynamicResultSet() throws Exception {
        ResultSet rs;
        methodWatcher.executeUpdate(CREATE_PROC_RETRIEVE_DYNAMIC_RESULTS);
        rs = methodWatcher.executeQuery(String.format("CALL %s.RETRIEVE_DYNAMIC_RESULTS(1)", SCHEMA_NAME));
        Assert.assertEquals("Incorrect # of rows returned!", 1, resultSetSize(rs));
        rs.close();
        methodWatcher.executeUpdate(DROP_PROC_RETRIEVE_DYNAMIC_RESULTS);
    }

    /**
     * Tests that <code>CallableStatement.execute()</code> and
     * <code> Statement.getMoreResults()</code>succeeds when
     * multiple result set is returned from a stored procedure.
     * @exception SQLException if a database error occurs
     */
    @Test
    public void testExecuteProcWithMultipleDynamicResultSet() throws Exception{
        ResultSet rs;
        int rsCnt = 0;
        methodWatcher.executeUpdate(CREATE_PROC_RETRIEVE_DYNAMIC_RESULTS);
        CallableStatement cStmt = methodWatcher.prepareCall(String.format("CALL %s.RETRIEVE_DYNAMIC_RESULTS(?)",SCHEMA_NAME));
        cStmt.setInt(1,2);
        cStmt.execute();

        rs = cStmt.getResultSet();
        int expectedRsCnt = 2;
        int expectedVal = 1;
        while(rs.next()){
            int val = rs.getInt(1);
            Assert.assertEquals("Incorrect returned value", expectedVal, val);
            expectedVal++;
        }
        rsCnt++;
        while(cStmt.getMoreResults()){
            rs = cStmt.getResultSet();
            rsCnt++;
            while(rs.next()){
                int val = rs.getInt(1);
                Assert.assertEquals("Incorrect returned value", expectedVal, val);
                expectedVal++;
            }
        }

        Assert.assertEquals("Incorrect number of result sets returned", expectedRsCnt, rsCnt);
        methodWatcher.executeUpdate(DROP_PROC_RETRIEVE_DYNAMIC_RESULTS);
        return;
    }

    /**
     * Test Python stored procedure with non-default connection
     * @exception SQLException if a database error occurs
     */
    @Test
    public void testDynamicResultSetsFromNotDefaulConnection() throws Exception{
        ResultSet rs;
        int rsCnt = 0;
        methodWatcher.executeUpdate(CREATE_PROC_RETRIEVE_EXTERNAL_RESULT);
        CallableStatement cStmt = methodWatcher.prepareCall(String.format("CALL %s.RETRIEVE_EXTERNAL_RESULT(?,?,?)",SCHEMA_NAME));
        cStmt.setString(1, TEST_DBNAME);
        cStmt.setString(2, TEST_USERNAME);
        cStmt.setString(3, TEST_PASSWORD);
        cStmt.execute();
        rs = cStmt.getResultSet();

        while(rs.next()){
            int val = rs.getInt(1);
            Assert.assertEquals("Incorrect returned value", 1, val);
            rsCnt++;
        }
        Assert.assertEquals("Incorrect number of rows for the result set returned", 1, rsCnt);

        if(cStmt.getMoreResults()){
            Assert.fail("Incorrect number of result sets returned");
        }
        methodWatcher.executeUpdate(DROP_PROC_RETRIEVE_EXTERNAL_RESULT);
        return;
    }

    /*
     * Test retrieve rows from ResultSet which has been closed in the Python Stored procedure
     */
    @Test
    public void testClosedDynamicResultSetsFromExecuteQuery() throws Exception{
        int rsCnt = 0;
        methodWatcher.executeUpdate(CREATE_PROC_RETRIEVE_CLOSED_RESULT);
        try{
            methodWatcher.executeQuery(String.format("CALL %s.RETRIEVE_CLOSED_RESULT()",SCHEMA_NAME));
            Assert.fail("retrive result from closed ResultSet should fail.");
            methodWatcher.executeUpdate(DROP_PROC_RETRIEVE_CLOSED_RESULT);
        } catch (SQLException sqle){
            methodWatcher.executeUpdate(DROP_PROC_RETRIEVE_CLOSED_RESULT);
            Assert.assertEquals("X0Y78", sqle.getSQLState());
            return;
        }
    }

    /*
     * Test stored procedure that only sets OUT parameter without returning any ResultSet
     */
    @Test
    public void testGetIntFromOutParameter() throws Exception{
        methodWatcher.executeUpdate(CREATE_PROC_INT_OUT);
        CallableStatement cs = methodWatcher.prepareCall(String.format("call %s.INT_OUT(?)",SCHEMA_NAME));
        cs.registerOutParameter(1, Types.INTEGER);
        cs.execute();
        int outVal = cs.getInt(1);
        Assert.assertEquals(42, outVal);
        methodWatcher.executeUpdate(DROP_PROC_INT_OUT);
    }

    /*
     * Test stored procedure using primitive types as OUTPUT parameter
     */
    @Test
    public void testGetPrimitiveFromOutParameter() throws Exception{
        methodWatcher.executeUpdate(CREATE_PROC_PRIM_OUT);
        CallableStatement cs = methodWatcher.prepareCall(String.format("call %s.PRIM_OUT(?,?,?,?,?,?,?)", SCHEMA_NAME));
        cs.registerOutParameter(1, Types.TINYINT);
        cs.registerOutParameter(2, Types.SMALLINT);
        cs.registerOutParameter(3, Types.BIGINT);
        cs.registerOutParameter(4, Types.BOOLEAN);
        cs.registerOutParameter(5, Types.DOUBLE);
        cs.registerOutParameter(6, Types.REAL);
        cs.registerOutParameter(7, Types.INTEGER);
        cs.execute();
        Assert.assertEquals(cs.getByte(1), 1);
        Assert.assertEquals(cs.getShort(2), 32767);
        Assert.assertEquals(cs.getLong(3), 9223372036854775807L);
        Assert.assertEquals(cs.getBoolean(4), true);
        // Since Assert.assertEquals(double, double) and Assert.assertEquals(float, float) deprecated
        // does not compare the retrieved value in the tests currently.
        cs.getDouble(5);
        cs.getFloat(6);
        Assert.assertEquals(cs.getInt(7), 2147483647);
        methodWatcher.executeUpdate(DROP_PROC_PRIM_OUT);
    }
}
