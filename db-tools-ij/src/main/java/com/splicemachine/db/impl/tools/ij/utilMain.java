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

package com.splicemachine.db.impl.tools.ij;
                
import com.splicemachine.db.iapi.services.info.ProductGenusNames;
import com.splicemachine.db.iapi.tools.i18n.*;
import com.splicemachine.db.tools.JDBCDisplayUtil;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.*;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.StringReader;
import java.sql.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Hashtable;
import java.util.List;
import java.util.Stack;

/**
	This class is utilities specific to the two ij Main's.
	This factoring enables sharing the functionality for
	single and dual connection ij runs.

 */
@SuppressFBWarnings(value = "NM_CLASS_NAMING_CONVENTION",justification = "DB-9772")
public class utilMain implements java.security.PrivilegedAction {

	private StatementFinder[] commandGrabber;
	UCode_CharStream charStream;
	ijTokenManager ijTokMgr;
	ij ijParser;
	ConnectionEnv[] connEnv;
	private int currCE;
	private final int numConnections;
	private boolean fileInput;
	private boolean initialFileInput;
	private boolean firstRun = true;
	private LocalizedOutput out = null;
	private Hashtable ignoreErrors;
	private boolean doSpool = false;
	private boolean omitHeader = false;
	private String logFileName = null;
	/**
	 * True if to display the error code when
	 * displaying a SQLException.
	 */
	private final boolean showErrorCode;
    
    /**
     * Value of the system property ij.execptionTrace
     */
    private final String ijExceptionTrace;

	/*
		In the goodness of time, this could be an ij property
	 */
	public static final int BUFFEREDFILESIZE = 2048;

	private static boolean showPromptClock = false;

	/*
	 * command can be redirected, so we stack up command
	 * grabbers as needed.
	 */
	Stack oldGrabbers = new Stack();

	LocalizedResource langUtil = LocalizedResource.getInstance();
	/**
	 * Set up the test to run with 'numConnections' connections/users.
	 *
	 * @param numConnections	The number of connections/users to test.
	 */
	utilMain(int numConnections, LocalizedOutput out)
		throws ijFatalException
	{
		this(numConnections, out, (Hashtable)null);
	}

    /**
	 * Set up the test to run with 'numConnections' connections/users.
     * This overload allows the choice of whether the system properties
     * will be used or not.
	 *
	 * @param numConnections	The number of connections/users to test.
	 */
    utilMain(int numConnections, LocalizedOutput out, boolean loadSystemProperties)
		throws ijFatalException
	{
		this(numConnections, out, (Hashtable)null);
        if (loadSystemProperties) {
            initFromEnvironment();
        }
	}

	/**
	 * Set up the test to run with 'numConnections' connections/users.
	 *
	 * @param numConnections	The number of connections/users to test.
	 * @param ignoreErrors		A list of errors to ignore.  If null,
	 *							all errors are printed out and nothing
	 *							is fatal.  If non-null, if an error is
	 *							hit and it is in this list, it is silently	
	 *							ignore.  Otherwise, an ijFatalException is
	 *							thrown.  ignoreErrors is used for stress
	 *							tests.
	 */
	@SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional, ignoreErrors (which is causing this) is used only for testing")
	public utilMain(int numConnections, LocalizedOutput out, Hashtable ignoreErrors)
		throws ijFatalException
	{
		/* init the parser; give it no input to start with.
		 * (1 parser for entire test.)
		 */
		charStream = new UCode_CharStream(
						new StringReader(" "), 1, 1);
		ijTokMgr = new ijTokenManager(charStream);
		ijParser = new ij(ijTokMgr, this);
		this.out = out;
		this.ignoreErrors = ignoreErrors;
		
		showErrorCode = 
			Boolean.valueOf(
					util.getSystemProperty("ij.showErrorCode")
					).booleanValue();
        
        ijExceptionTrace = util.getSystemProperty("ij.exceptionTrace");

		this.numConnections = numConnections;
		/* 1 StatementFinder and ConnectionEnv per connection/user. */
		commandGrabber = new StatementFinder[numConnections];
		connEnv = new ConnectionEnv[numConnections];

		for (int ictr = 0; ictr < numConnections; ictr++)
		{
		    commandGrabber[ictr] = new StatementFinder(langUtil.getNewInput(System.in), out);
			connEnv[ictr] = new ConnectionEnv(ictr, (numConnections > 1), (numConnections == 1));
		}

		/* Start with connection/user 0 */
		currCE = 0;
		fileInput = false;
		initialFileInput = false;
		firstRun = true;
	}
	
	/**
	 * Initialize the connections from the environment.
	 *
	 */
	public void initFromEnvironment()
	{
		ijParser.initFromEnvironment();
		
		for (int ictr = 0; ictr < numConnections; ictr++)
		{
			try {
				connEnv[ictr].init(out);
			} catch (SQLException s) {
				JDBCDisplayUtil.ShowException(out, s); // will continue past connect failure
			} catch (ClassNotFoundException c) {
				JDBCDisplayUtil.ShowException(out, c); // will continue past driver failure
			} catch (InstantiationException i) {
				JDBCDisplayUtil.ShowException(out, i); // will continue past driver failure
			} catch (IllegalAccessException ia) {
				JDBCDisplayUtil.ShowException(out, ia); // will continue past driver failure
			}
		}
	}


	/**
	 * run ij over the specified input, sending output to the
	 * specified output. Any prior input and output will be lost.
	 *
	 * @param in source for input to ij
	 * @param out sink for output from ij
	 */
	public void go(LocalizedInput[] in, LocalizedOutput out)
				   throws ijFatalException
	{
		this.out = out;
		
		ijParser.setConnection(connEnv[currCE], (numConnections > 1));
		fileInput = initialFileInput = (!in[currCE].isStandardInput());

		for (int ictr = 0; ictr < commandGrabber.length; ictr++) {
			commandGrabber[ictr].reInit(in[ictr]);
		}

		if (firstRun) {
			for (int i=connEnv.length-1;i>=0;i--) { // print out any initial warnings...
				Connection c = connEnv[i].getConnection();
				if (c!=null) {
					JDBCDisplayUtil.ShowWarnings(out,c);
				}
			}
			firstRun = false;

			supportIJProperties(connEnv[currCE]);
    	}
		runScriptGuts();
		cleanupGo(in);
	}
	
	/**
	 * Support to run a script. Performs minimal setup
	 * to set the passed in connection into the existing
	 * ij setup, ConnectionEnv.
	 * @param conn
	 * @param in
	 */
	public int goScript(Connection conn,
			LocalizedInput in)
	{
	    connEnv[0].addSession(conn, (String) null);
        ijParser.setConnection(connEnv[0], (numConnections > 1));
	    supportIJProperties(connEnv[0]);   
	    		
		fileInput = initialFileInput = !in.isStandardInput();
		commandGrabber[0].reInit(in);
		return runScriptGuts();
	}
	
	private void supportIJProperties(ConnectionEnv env) {
	  //check if the property is set to not show select count and set the static variable
        //accordingly. 
        boolean showNoCountForSelect = Boolean.valueOf(util.getSystemProperty("ij.showNoCountForSelect")).booleanValue();
        JDBCDisplayUtil.showSelectCount = !showNoCountForSelect;

        //check if the property is set to not show initial connections and accordingly set the
        //static variable.
        boolean showNoConnectionsAtStart = Boolean.valueOf(util.getSystemProperty("ij.showNoConnectionsAtStart")).booleanValue();

        if (!(showNoConnectionsAtStart)) {
            try {
                ijResult result = ijParser.showConnectionsMethod(true);
                displayResult(out,result,env.getConnection());
            } catch (SQLException ex) {
                handleSQLException(out,ex);
            }
        }        
    }

    /**
	 * Run the guts of the script. Split out to allow
	 * calling from the full ij and the minimal goScript.
     * @return The number of errors seen in the script.
	 *
	 */
	private int runScriptGuts() {

        int scriptErrorCount = 0;
		
		boolean done = false;
		String command = null;
		while (!ijParser.exit && !done) {
			try{
				ijParser.setConnection(connEnv[currCE], (numConnections > 1));
			} catch(Throwable t){
				//do nothing
				}

			connEnv[currCE].doPrompt(true, out);
   			try {
   				command = null;
				out.flush();
				command = commandGrabber[currCE].nextStatement();

				if(doSpool) {
					assert out.getOutputStream() instanceof ForkOutputStream;
					ForkOutputStream fos = (ForkOutputStream)(out.getOutputStream());
					fos.setWriteToOut(false); // do not write the command twice to std output
					try {
						out.println(command + ";");
						out.flush();
					}
					finally {
						fos.setWriteToOut(true);
					}
				}

				// if there is no next statement,
				// pop back to the top saved grabber.
				while (command == null && ! oldGrabbers.empty()) {
					// close the old input file if not System.in
					if (fileInput) commandGrabber[currCE].close();
					commandGrabber[currCE] = (StatementFinder)oldGrabbers.pop();
					if (oldGrabbers.empty())
						fileInput = initialFileInput;
					command = commandGrabber[currCE].nextStatement();
				}

				// if there are no grabbers left,
				// we are done.
				if (command == null && oldGrabbers.empty()) {
					done = true;
				}
				else {
					boolean	elapsedTimeOn = ijParser.getElapsedTimeState();
					long	beginTime = 0;
					long	endTime;

					if (fileInput && !doSpool) {
						out.println(command+";");
						out.flush();
					}

					assert command != null;
					charStream.ReInit(new StringReader(command), 1, 1);
					ijTokMgr.ReInit(charStream);
					ijParser.ReInit(ijTokMgr);

					if (elapsedTimeOn) {
						beginTime = System.currentTimeMillis();
					}

					ijResult result = ijParser.ijStatement();

					if(!(result instanceof ijShellConfigResult)) {
						displayResult(out,result,connEnv[currCE].getConnection());

						// if something went wrong, an SQLException or ijException was thrown.
						// we can keep going to the next statement on those (see catches below).
						// ijParseException means we try the SQL parser.

						/* Print the elapsed time if appropriate */
						if (elapsedTimeOn) {
							endTime = System.currentTimeMillis();
							out.println(langUtil.getTextMessage("IJ_ElapTime0Mil",
									langUtil.getNumberAsString(endTime - beginTime)));
						}

						// would like when it completes a statement
						// to see if there is stuff after the ;
						// and before the <EOL> that we will IGNORE
						// (with a warning to that effect)
					}
				}

    			} catch (ParseException e) {
				    scriptErrorCount += doCatch(command) ? 0 : 1;
				} catch (TokenMgrError e) {
 					if (command != null)
                        scriptErrorCount += doCatch(command) ? 0 : 1;
    			} catch (SQLException e) {
                    scriptErrorCount++;
					// SQL exception occurred in ij's actions; print and continue
					// unless it is considered fatal.
					handleSQLException(out,e);
    			} catch (ijException e) {
                    scriptErrorCount++;
					// exception occurred in ij's actions; print and continue
    			  	out.println(langUtil.getTextMessage("IJ_IjErro0",e.getMessage()));
					doTrace(e);
    			} catch (Throwable e) {
                    scriptErrorCount++;
    			  	out.println(langUtil.getTextMessage("IJ_JavaErro0",e.toString()));
					doTrace(e);
				}

			/* Go to the next connection/user, if there is one */
			currCE = ++currCE % connEnv.length;
		}
        
        return scriptErrorCount;
	}
	
	/**
	 * Perform cleanup after a script has been run.
	 * Close the input streams if required and shutdown
	 * db on an exit.
	 * @param in
	 */
	private void cleanupGo(LocalizedInput[] in) {

		// we need to close all sessions when done; otherwise we have
		// a problem when a single VM runs successive IJ threads
		try {
			for (int i = 0; i < connEnv.length; i++) {
				connEnv[i].removeAllSessions();
			}
		} catch (SQLException se ) {
			handleSQLException(out,se);
		}
		// similarly must close input files
		for (int i = 0; i < numConnections; i++) {
			try {
				in[i].close();	
			} catch (Exception e ) {
    			  	out.println(langUtil.getTextMessage("IJ_CannotCloseInFile",
					e.toString()));
			}
		}

		/*
			If an exit was requested, then we will be shutting down.
		 */
//		if (ijParser.exit || (initialFileInput && !mtUse)) {
//        	if(!AutoloadedDriver.isBooted()) return; //no reason to try booting the db if we are just going to shut it down
//			Driver d = null;
//			try {
//			    d = DriverManager.getDriver("jdbc:splice:");
//			} catch (Throwable e) {
//				d = null;
//			}
//			if (d!=null) { // do we have a driver running? shutdown on exit.
//				try {
//					DriverManager.getConnection("jdbc:splice:;shutdown=true");
//				} catch (SQLException e) {
//					// ignore the errors, they are expected.
//				}
//			}
//		}
  	}

	private void displayResult(LocalizedOutput out, ijResult result, Connection conn) throws SQLException {
		// display the result, if appropriate.
		if (result!=null) {
			if (result.isConnection()) {
				if (result.hasWarnings()) {
					JDBCDisplayUtil.ShowWarnings(out,result.getSQLWarnings());
					result.clearSQLWarnings();
				}
			} else if (result.isStatement()) {
				Statement s = result.getStatement();
				try {
				    JDBCDisplayUtil.DisplayResults(out,s,connEnv[currCE].getConnection(), omitHeader);
				} catch (SQLException se) {
				    result.closeStatement();
					throw se;
				}
				result.closeStatement();
			} else if (result.isNextRowOfResultSet()) {
				ResultSet r = result.getNextRowOfResultSet();
				JDBCDisplayUtil.DisplayCurrentRow(out,r,connEnv[currCE].getConnection(), omitHeader);
			} else if (result.isVector()) {
				util.displayVector(out,result.getVector());
				if (result.hasWarnings()) {
					JDBCDisplayUtil.ShowWarnings(out,result.getSQLWarnings());
					result.clearSQLWarnings();
				}
			} else if (result.isMulti()) {
			    try {
				    util.displayMulti(out,(PreparedStatement)result.getStatement(),result.getResultSet(),connEnv[currCE].getConnection(), omitHeader);
				} catch (SQLException se) {
				    result.closeStatement();
					throw se;
				}
				result.closeStatement(); // done with the statement now
				if (result.hasWarnings()) {
					JDBCDisplayUtil.ShowWarnings(out,result.getSQLWarnings());
					result.clearSQLWarnings();
				}
			} else if (result.isResultSet()) {
				ResultSet rs = result.getResultSet();
				try {
					JDBCDisplayUtil.DisplayResults(out,rs,connEnv[currCE].getConnection(), result.getColumnDisplayList(), result.getColumnWidthList(), omitHeader);
				} catch (SQLException se) {
					result.closeStatement();
					throw se;
				}
				result.closeStatement();
            } else if (result.isMultipleResultSetResult()) {
              List resultSets = result.getMultipleResultSets();
              try {
                JDBCDisplayUtil.DisplayMultipleResults(out,resultSets,
                                     connEnv[currCE].getConnection(),
                                     result.getColumnDisplayList(),
                                     result.getColumnWidthList(),
						             omitHeader);
              } catch (SQLException se) {
                result.closeStatement();
                throw se;
              }
			} else if (result.isException()) {
				JDBCDisplayUtil.ShowException(out,result.getException());
			} else if (result.isUnsupportedCommand())
			{
				out.println(result);
			}	
		}
	}

	/**
	 * catch processing on failed commands. This really ought to
	 * be in ij somehow, but it was easier to catch in Main.
	 */
	private boolean doCatch(String command) {
		// this retries the failed statement
		// as a JSQL statement; it uses the
		// ijParser since that maintains our
		// connection and state.

        
	    try {
			boolean	elapsedTimeOn = ijParser.getElapsedTimeState();
			long	beginTime = 0;
			long	endTime;

			if (elapsedTimeOn) {
				beginTime = System.currentTimeMillis();
			}

			ijResult result = ijParser.executeImmediate(command);
			displayResult(out,result,connEnv[currCE].getConnection());

			/* Print the elapsed time if appropriate */
			if (elapsedTimeOn) {
				endTime = System.currentTimeMillis();
				out.println(langUtil.getTextMessage("IJ_ElapTime0Mil_4", 
				langUtil.getNumberAsString(endTime - beginTime)));
			}
            return true;

	    } catch (SQLException e) {
			// SQL exception occurred in ij's actions; print and continue
			// unless it is considered fatal.
			handleSQLException(out,e);
	    } catch (ijException i) {
	  		out.println(langUtil.getTextMessage("IJ_IjErro0_5", i.getMessage()));
			doTrace(i);
		} catch (ijTokenException ie) {
	  		out.println(langUtil.getTextMessage("IJ_IjErro0_6", ie.getMessage()));
			doTrace(ie);
	    } catch (Throwable t) {
	  		out.println(langUtil.getTextMessage("IJ_JavaErro0_7", t.toString()));
			doTrace(t);
	    }
        return false;
	}

	/**
	 * This routine displays SQL exceptions and decides whether they
	 * are fatal or not, based on the ignoreErrors field. If they
	 * are fatal, an ijFatalException is thrown.
	 * Lifted from ij/util.java:ShowSQLException
	 */
	private void handleSQLException(LocalizedOutput out, SQLException e) 
		throws ijFatalException
	{
		String errorCode;
		String sqlState = null;
		SQLException fatalException = null;

		if (showErrorCode) {
			errorCode = langUtil.getTextMessage("IJ_Erro0", 
			langUtil.getNumberAsString(e.getErrorCode()));
		}
		else {
			errorCode = "";
		}

		boolean syntaxErrorOccurred = false;
		for (; e!=null; e=e.getNextException())
		{
			sqlState = e.getSQLState();
			if ("42X01".equals(sqlState))
				syntaxErrorOccurred = true;
			/*
			** If we are to throw errors, then throw the exceptions
			** that aren't in the ignoreErrors list.  If
			** the ignoreErrors list is null we don't throw
			** any errors.
			*/
		 	if (ignoreErrors != null) 
			{
				if ((sqlState != null) &&
					(ignoreErrors.get(sqlState) != null))
				{
					continue;
				}
				else
				{
					fatalException = e;
				}
			}
			String st1 = JDBCDisplayUtil.mapNull(e.getSQLState(),langUtil.getTextMessage("IJ_NoSqls"), true);
			String st2 = JDBCDisplayUtil.mapNull(e.getMessage(),langUtil.getTextMessage("IJ_NoMess"));
			out.println(langUtil.getTextMessage("IJ_Erro012",  st1, st2, errorCode));
			doTrace(e);
		}
		if (fatalException != null)
		{
			throw new ijFatalException(fatalException);
		}
		if (syntaxErrorOccurred)
			out.println(langUtil.getTextMessage("IJ_SuggestHelp"));
	}

	/**
	 * stack trace dumper
	 */
	private void doTrace(Throwable t) {
		if (ijExceptionTrace != null) {
			t.printStackTrace(out);
		}
		out.flush();
	}

	void newInput(String fileName) {
		FileInputStream newFile = null;
		try {
			newFile = new FileInputStream(fileName);
      	} catch (FileNotFoundException e) {
        	throw ijException.fileNotFound();
		}

		// if the file was opened, move to use it for input.
		oldGrabbers.push(commandGrabber[currCE]);
	    commandGrabber[currCE] = 
                new StatementFinder(langUtil.getNewInput(new BufferedInputStream(newFile, BUFFEREDFILESIZE)), null);
		fileInput = true;
	}

	void newResourceInput(String resourceName) {
		InputStream is = util.getResourceAsStream(resourceName);
		if (is==null) throw ijException.resourceNotFound();
		oldGrabbers.push(commandGrabber[currCE]);
	    commandGrabber[currCE] = 
                new StatementFinder(langUtil.getNewEncodedInput(new BufferedInputStream(is, BUFFEREDFILESIZE), "UTF8"), null);
		fileInput = true;
	}

	/**
	 * REMIND: eventually this might be part of StatementFinder,
	 * used at each carriage return to show that it is still "live"
	 * when it is reading multi-line input.
	 */
	 static void doPrompt(boolean newStatement, LocalizedOutput out, String tag)
	 {
	 	final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZZZZ");

	 	String clock = showPromptClock ? " " + sdf.format(new Timestamp(System.currentTimeMillis())) : "";

		if (newStatement) {
	  		out.print("splice" + clock + "> ");
		}
		else {
			out.print(clock + "> ");
		}
		out.flush();
	}

    /**
     * Check that the cursor is scrollable.
     *
     * @param rs the ResultSet to check
     * @param operation which operation this is checked for
     * @exception ijException if the cursor isn't scrollable
     * @exception SQLException if a database error occurs
     */
    private void checkScrollableCursor(ResultSet rs, String operation)
            throws ijException, SQLException {
        if (rs.getType() == ResultSet.TYPE_FORWARD_ONLY) {
            throw ijException.forwardOnlyCursor(operation);
        }
    }

	/**
	 * Position on the specified row of the specified ResultSet.
	 *
	 * @param rs	The specified ResultSet.
	 * @param row	The row # to move to.
	 *				(Negative means from the end of the result set.)
	 *
	 * @return	NULL.
	 *
	 * @exception	SQLException thrown on error.
	 *				(absolute() not supported pre-JDBC2.0)
	 */
	ijResult absolute(ResultSet rs, int row)
		throws SQLException
	{
        checkScrollableCursor(rs, "ABSOLUTE");
		// 0 is an *VALID* value for row
		return new ijRowResult(rs, rs.absolute(row));
	}

	/**
	 * Move the cursor position by the specified amount.
	 *
	 * @param rs	The specified ResultSet.
	 * @param row	The # of rows to move.
	 *				(Negative means toward the beginning of the result set.)
	 *
	 * @return	NULL.
	 *
	 * @exception	SQLException thrown on error.
	 *				(relative() not supported pre-JDBC2.0)
	 */
	ijResult relative(ResultSet rs, int row)
		throws SQLException
	{
        checkScrollableCursor(rs, "RELATIVE");
		return new ijRowResult(rs, rs.relative(row));
	}

	/**
	 * Position before the first row of the specified ResultSet
	 * and return NULL to the user.
	 *
	 * @param rs	The specified ResultSet.
	 *
	 * @return	NULL.
	 *
	 * @exception	SQLException thrown on error.
	 *				(beforeFirst() not supported pre-JDBC2.0)
	 */
	ijResult beforeFirst(ResultSet rs)
		throws SQLException
	{
        checkScrollableCursor(rs, "BEFORE FIRST");
		rs.beforeFirst();
		return new ijRowResult(rs, false);
	}

	/**
	 * Position on the first row of the specified ResultSet
	 * and return that row to the user.
	 *
	 * @param rs	The specified ResultSet.
	 *
	 * @return	The first row of the ResultSet.
	 *
	 * @exception	SQLException thrown on error.
	 *				(first() not supported pre-JDBC2.0)
	 */
	ijResult first(ResultSet rs)
		throws SQLException
	{
        checkScrollableCursor(rs, "FIRST");
		return new ijRowResult(rs, rs.first());
	}

	/**
	 * Position after the last row of the specified ResultSet
	 * and return NULL to the user.
	 *
	 * @param rs	The specified ResultSet.
	 *
	 * @return	NULL.
	 *
	 * @exception	SQLException thrown on error.
	 *				(afterLast() not supported pre-JDBC2.0)
	 */
	ijResult afterLast(ResultSet rs)
		throws SQLException
	{
        checkScrollableCursor(rs, "AFTER LAST");
		rs.afterLast();
		return new ijRowResult(rs, false);
	}

	/**
	 * Position on the last row of the specified ResultSet
	 * and return that row to the user.
	 *
	 * @param rs	The specified ResultSet.
	 *
	 * @return	The last row of the ResultSet.
	 *
	 * @exception	SQLException thrown on error.
	 *				(last() not supported pre-JDBC2.0)
	 */
	ijResult last(ResultSet rs)
		throws SQLException
	{
        checkScrollableCursor(rs, "LAST");
		return new ijRowResult(rs, rs.last());
	}

	/**
	 * Position on the previous row of the specified ResultSet
	 * and return that row to the user.
	 *
	 * @param rs	The specified ResultSet.
	 *
	 * @return	The previous row of the ResultSet.
	 *
	 * @exception	SQLException thrown on error.
	 *				(previous() not supported pre-JDBC2.0)
	 */
	ijResult previous(ResultSet rs)
		throws SQLException
	{
        checkScrollableCursor(rs, "PREVIOUS");
		return new ijRowResult(rs, rs.previous());
	}

	/**
	 * Get the current row number
	 *
	 * @param rs	The specified ResultSet.
	 *
	 * @return	The current row number
	 *
	 * @exception	SQLException thrown on error.
	 *				(getRow() not supported pre-JDBC2.0)
	 */
	int getCurrentRowNumber(ResultSet rs)
		throws SQLException
	{
        checkScrollableCursor(rs, "GETCURRENTROWNUMBER");
		return rs.getRow();
	}

	public final Object run() {
		return  getClass().getResourceAsStream(ProductGenusNames.TOOLS_INFO);
	}

	public static void setPromptClock(boolean showPromptClock) {
		utilMain.showPromptClock = showPromptClock;
	}

	@SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED_BAD_PRACTICE",justification = "Intentional, we don't care if the file already exists")
	public void startSpooling(String path) throws IOException {
		try {
			logFileName = path;
			File f = new File(logFileName);
			if(!f.exists()) {
				f.createNewFile(); // create if not exists, no-op if file exists.
				out.println(langUtil.getTextMessage("IJ_SpoolNewFile", path));
			} else if(f.exists() && f.canWrite() && !f.isDirectory()) {
				out.println(langUtil.getTextMessage("IJ_SpoolFileAlreadyExistsWarning", path));
			} else {
				out.println(langUtil.getTextMessage("IJ_SpoolError", path));
				return;
			}
			this.out.close();
			this.out = new LocalizedOutput(new ForkOutputStream(new FileOutputStream(f)));
			doSpool = true;
		} catch (IOException e) {
			out.println(langUtil.getTextMessage("IJ_SpoolError", path));
		}
	}

	public void stopSpooling() {
		if(!doSpool) {
			out.println(langUtil.getTextMessage("IJ_SpoolNotActive"));
		} else {
			doSpool = false;
			this.out.flush();
			this.out.close();
			this.out = new LocalizedOutput(System.out);
		}
	}

	public void clearSpooling() throws IOException {
		if(doSpool)
		{
			PrintWriter pw = new PrintWriter(logFileName, "UTF-8");
			pw.close();
		} else {
			out.println(langUtil.getTextMessage("IJ_SpoolNotActive"));
		}
	}

	public void setOmitHeader(boolean omitHeader) {
		this.omitHeader = omitHeader;
	}
}
