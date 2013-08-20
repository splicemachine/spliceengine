package com.splicemachine.test.statement;

/**
 * @author Jeff Cunningham
 *         Date: 7/23/13
 */

import java.io.*;
import java.sql.*;

/**
 * <p>
 * This program runs a script of SQL statements, stopping if an error occurs.
 * </p>
 */
public class SQLRunner {
    /////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    /////////////////////////////////////////////////////////

    public static final String USAGE =
            "USAGE:\n" +
                    "\n" +
                    "java SQLRunner connectionURL scriptName\n" +
                    "\n" +
                    "E.g.:\n" +
                    "\n" +
                    "   java SQLRunner \"jdbc:derby:memory:db;create=true\" \"/Users/me/sql/w.sql\"";

    /////////////////////////////////////////////////////////
    //
    // STATE
    //
    /////////////////////////////////////////////////////////

    private String _connectionURL;
    private File _scriptFile;

    /////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    /////////////////////////////////////////////////////////

    /**
     * Create from a script file
     */
    public SQLRunner(String connectionURL, File scriptFile) {
        _connectionURL = connectionURL;
        _scriptFile = scriptFile;
    }

    /////////////////////////////////////////////////////////
    //
    // ENTRY POINT
    //
    /////////////////////////////////////////////////////////

    /**
     * Takes one argument, the name of the script file
     */
    public static void main(String... args) {
        SQLRunner runner = parseArgs(args);
        if (runner == null) {
            println(USAGE);
            exit(1);
        }

        runner.execute();
    }

    private static SQLRunner parseArgs(String... args) {
        if (args == null) {
            return null;
        }
        if (args.length != 2) {
            return null;
        }

        int idx = 0;
        String connectionURL = args[idx++];
        String fileName = args[idx++];

        if (connectionURL.length() == 0) {
            return null;
        }
        if (fileName.length() == 0) {
            return null;
        }

        return new SQLRunner(connectionURL, new File(fileName));
    }

    /////////////////////////////////////////////////////////
    //
    // WORKHORSE
    //
    /////////////////////////////////////////////////////////

    /**
     * Main execution loop
     */
    private void execute() {
        try {
            Connection conn = DriverManager.getConnection(_connectionURL);
            FileInputStream fis = new FileInputStream(_scriptFile);
            StatementReader reader = new StatementReader(fis, null);

            while (true) {
                String nextStatement = reader.nextStatement();

                println(nextStatement);
                conn.prepareStatement(nextStatement).execute();
            }
        } catch (Throwable t) {
            printThrowable(t);
            exit(1);
        } finally {
            try {
                DriverManager.getConnection("jdbc:derby:;shutdown=true");
            } catch (Throwable t) {
                println(t.getMessage());
            }
        }
    }

    /////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    /////////////////////////////////////////////////////////

    /**
     * Exit the program with an error status
     */
    private static void exit(int error) {
        System.exit(error);
    }

    /**
     * Print a message
     */
    private static void println(String text) {
        System.out.println(text);
    }

    /**
     * Print an exception
     */
    private static void printThrowable(Throwable t) {
        println(t.getMessage());
        t.printStackTrace(System.out);
    }


}