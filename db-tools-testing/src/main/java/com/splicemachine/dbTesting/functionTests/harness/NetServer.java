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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.dbTesting.functionTests.harness;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Locale;
import java.util.Vector;
import java.util.Hashtable;
import java.lang.reflect.Method;
import java.lang.reflect.Constructor;
import java.net.Socket;
import com.splicemachine.dbTesting.functionTests.util.TestUtil;

public class NetServer
{

    File homeDir; // The server directory (usually the test directory)
    String jvmName = "jdk13";
    String clPath;
    String javaCmd;
    String jvmflags;
    String framework;
	String appsRequiredPassword;
    static String hostName;
    
    Object[] frameworkInfo;
    int port;
    Process pr;
    BackgroundStreamSaver outSaver, errSaver;
    FileOutputStream fosOut, fosErr;
	private boolean startServer;  // whether test will start it's own server

	//  Variables for test connection
    Object networkServer;   // Server needs to be created with reflection
	Method pingMethod;

	private static String NETWORK_SERVER_CLASS_NAME="com.splicemachine.db.drda.NetworkServerControl";
    
    public static Hashtable m;
    public static int PREFIX_POS = 0;
    public static int SUFFIX_POS = 1;
    public static int DRIVER_POS = 2;
    public static int PORT_POS = 3;
    public static int START_CMD_POS = 4;
    public static int STOP_CMD1_POS = 5;
    public static int STOP_CMD2_POS = 6;

    
    static {
    	hostName=TestUtil.getHostName();
	m =  new Hashtable();
	// Hashtable is keyed on framework name and has 
	// an array of the framework prefix, suffix, driver, port  and 
	// String[] command arguments to start the server
	// String[] Command arguments to stop the server
	String url = "jdbc:splice:net://" + hostName + ":1527/";
	m.put("DerbyNet", new Object[]
	    {url,                 //prefix
	     "",                                            // suffix
	     "com.ibm.db2.jcc.DB2Driver",                   //driver
	     "1527",                                        // port
	     new String[] {NETWORK_SERVER_CLASS_NAME,  //start
			   "start"},                        
	     new String[] {NETWORK_SERVER_CLASS_NAME,  //shutdown
			   "shutdown"},
	     null});                                        //shutdown2

	url = "jdbc:splice://" + hostName + ":1527/";

	m.put("DerbyNetClient", new Object[]
	    {url,                 //prefix
	     "",                                            // suffix
	     "com.splicemachine.db.jdbc.ClientDriver",           //driver
	     "1527",                                        // port
	     new String[] {NETWORK_SERVER_CLASS_NAME,  //start
			   "start"},                        
	     new String[] {NETWORK_SERVER_CLASS_NAME,  //shutdown
			   "shutdown"},
	     null});                                        //shutdown2

	url = "jdbc:db2://" + hostName + ":50000/";
	m.put("DB2jcc", new Object[]
	    {url,                //prefix
	     "",                                            //suffix
	     "com.ibm.db2.jcc.DB2Driver",                   //driver
	     "50000",                                       //port
	     null,                                          //start
	     null,                                          
	     null});

	m.put("DB2app", new Object[]
	    {"jdbc:db2:",
	     "",
	     "COM.ibm.db2.jdbc.app.DB2Driver",
	     "0",
	     null,
	     null,
	     null});
    }

    public NetServer(File homeDir, String jvmName, String clPath,
					 String javaCmd, String jvmflags, String framework,
					 boolean startServer, String appsRequiredPassword)
	throws Exception
    {
	this.homeDir = homeDir;
        this.jvmName = jvmName;
        this.clPath = clPath;
        this.javaCmd = javaCmd;
        this.jvmflags = jvmflags;
	this.framework = framework;

	    // if authentication is required to shutdown server we need password
	    // for user SPLICE (the dbo).
    	this.appsRequiredPassword = appsRequiredPassword;
	frameworkInfo =  (Object[]) m.get(framework);
	
	this.port = Integer.parseInt((String) frameworkInfo[PORT_POS]);
	this.startServer = startServer;
	// System.out.println("framework: " + this.framework + "port: " + this.port);
	
    }
    public void start() throws Exception
    {
      if (! startServer)
	  {
		System.out.println("startServer = false. Bypass server startup");
		return;
	  }

	// Create the Server directory under the    server dir
	(new File(homeDir, framework + "Server")).mkdir();
	String[] startcmd = (String[]) frameworkInfo[START_CMD_POS];
	// if we are just connecting to DB2 we return
	if (startcmd == null) 
	    return;
	
        // Build the command to run the WL server
	String homeDirName = homeDir.getCanonicalPath();
		jvm jvm = null; // to quiet the compiler
		jvm = jvm.getJvm(jvmName);
		if (jvmName.equals("jview"))
		    jvm.setJavaCmd("jview");
		else if (javaCmd != null)
		    jvm.setJavaCmd(javaCmd);
		
		Vector jvmProps = new Vector();
		if ( (clPath != null) && (clPath.length()>0) )
		    jvm.setClasspath(clPath);

        if ( (jvmflags != null) && (jvmflags.length()>0) ) {
            jvm.setFlags(jvmflags);
            // Set no flags by default (DERBY-1614).
            // The jvmflags property can be used to set any kind of JVM option.
        }

        jvmProps.addElement("derby.system.home=" + homeDirName);
		jvm.setD(jvmProps);
		jvm.setSecurityProps();
        // For some platforms (like Mac) the process exec command
        // must be a string array; so we build this with a Vector
        // first because some strings (paths) could have spaces
	Vector vCmd = jvm.getCommandLine();
	for (int i = 0; i < startcmd.length; i++)
	    vCmd.addElement(startcmd[i]);

	String serverCmd[] = new String[vCmd.size()];
	for (int i = 0; i < vCmd.size(); i++)
	{
	    serverCmd[i] = (String)vCmd.elementAt(i);
	    System.out.print(serverCmd[i] + " ");
	}
	System.out.println("");
        // Start a process to run the Server
	pr = Runtime.getRuntime().exec(serverCmd);
	
        // Write the out and err files to the server directory also
	File out = new File(homeDir, framework + ".out");
	fosOut = new FileOutputStream(out);
	outSaver = new BackgroundStreamSaver(pr.getInputStream(), fosOut);
	File err = new File(homeDir, framework + ".err");
	fosErr = new FileOutputStream(err);
	errSaver = new BackgroundStreamSaver(pr.getErrorStream(), fosErr);
	
	for (int i = 0 ; i <= 120 ; i++)
	{
	    // No need to wait for DB2
	    if (isDB2Connection(framework))
		break;
     
	    try
	    {
 			if (isNetworkServerConnection(framework))
			{
				// adding a testconnection check 
				// so that the test does not start before the server is up
 				if (testNetworkServerConnection())
					break;
 			}
 			else	
 			{
 				Socket s = new Socket(hostName, this.port);
 				s.close();
				break;
 			}

		}
		catch (Exception e)
	    {
		// bail out if something has been written to stderr
		if (err.length() > 0) {
		    break;
		} else {
				// it's probably unnecessary to sleep, since the
				// connection request generally takes a long time when
				// the listener hasn't started yet, but what the heck ...
		    Thread.sleep(1000);
				// but here we iterate, and after 120 seconds, we stop
				// waiting to connect.
		} 
		
	    }
	}
    }
    
	public boolean  testNetworkServerConnection() throws Exception
	{ 	
		if (! startServer)
		{
			System.out.println("startServer = false. Bypass server check");
			return true;
		}
		
	    Object[] testConnectionArg  = null;
		if (networkServer == null)
		{
			Constructor serverConstructor;
			Class serverClass = Class.forName(NETWORK_SERVER_CLASS_NAME);
			serverConstructor = serverClass.getConstructor(null);
			networkServer = serverConstructor.newInstance(null);
			pingMethod = networkServer.getClass().getMethod("ping",
															 null);
		}
		pingMethod.invoke(networkServer,null);
		return true;
	}

    // stop the Server
	public void stop() throws Exception
    {
	  if (! startServer)
	  {
		return;
	  }

	System.out.println("Attempt to shutdown framework: " 
						 + framework);
	jvm jvm = null; // to quiet the compiler
	jvm = jvm.getJvm(jvmName);
	Vector jvmCmd = jvm.getCommandLine();
	
	Vector connV = new Vector();
	for (int i = 0; i < jvmCmd.size(); i++)
	{
	    connV.addElement((String)jvmCmd.elementAt(i));
        }
	
	String[] stopcmd1 = (String[]) frameworkInfo[STOP_CMD1_POS];
		if (stopcmd1 == null)
		    return;

		if (appsRequiredPassword != null) {
			String[] modifiedStopCmd = new String[stopcmd1.length + 4];
			System.arraycopy(stopcmd1, 0, modifiedStopCmd, 0, stopcmd1.length);
			modifiedStopCmd[stopcmd1.length]     = "-user";
			modifiedStopCmd[stopcmd1.length + 1] = "app";
			modifiedStopCmd[stopcmd1.length + 2] = "-password";
			modifiedStopCmd[stopcmd1.length + 3] = appsRequiredPassword;
			stopcmd1 = modifiedStopCmd;
		}


		for (int i = 0; i < stopcmd1.length; i++)
		    connV.addElement(stopcmd1[i]);
		
		String[] connCmd = new String[connV.size()];
		for (int i = 0; i < connV.size(); i++)
		{
		    connCmd[i] = (String)connV.elementAt(i);
		}		    
		
		
		Vector stopV = new Vector();
		for (int i = 0; i < jvmCmd.size(); i++)
		{
		    stopV.addElement((String)jvmCmd.elementAt(i));
		}

		Process prconn = Runtime.getRuntime().exec(connCmd);
		// Give the server sixty seconds to shutdown.
		TimedProcess tp = new TimedProcess(prconn);
		tp.waitFor(60);
		
		String[] stopcmd2 = (String[]) frameworkInfo[STOP_CMD2_POS];
		if (stopcmd2 != null)
		{
		    for (int i = 0; i < stopcmd2.length; i++)
			stopV.addElement(stopcmd2[i]);
		    
		    String[] stopCmd = new String[stopV.size()];
		    for (int i = 0; i < stopV.size(); i++)
		    {
			stopCmd[i] = (String)stopV.elementAt(i);
		    }
		    
		    Process prstop = Runtime.getRuntime().exec(stopCmd);
		    prstop.waitFor();
		}

		// Try a TimedProcess as Phil did for the WLServer
		tp = new TimedProcess(pr);
		// In case the Server didn't shut down, force it to ...
		tp.waitFor(60);
		
		// Finish and close the redirected out and err files
		outSaver.finish();
		errSaver.finish();
	}
    
    public void printFramworkInfo(String framework)
    {
	System.out.println("PREFIX = " + frameworkInfo[PREFIX_POS]);
	System.out.println("SUFFIX = " + frameworkInfo[SUFFIX_POS]);
	System.out.println("DRIVER = " + frameworkInfo[DRIVER_POS]);
	System.out.println("PORT = " + frameworkInfo[PORT_POS]);
	
	for (int index = START_CMD_POS; index <= STOP_CMD2_POS; index++)
	{
	    String cmdString = "";
	    String[] cmdArray = (String[]) frameworkInfo[index] ;
	    for (int i = 0; i < cmdArray.length; i++)
	    {
		cmdString += " " + cmdArray[i];
	    }
	    if (index == START_CMD_POS)
		System.out.println("START_CMD =  " + cmdString);
	    else
		System.out.println("STOP_CMD = " + cmdString);
	    
	}
    }
    
    // Get Framework Info
    public static String getURLPrefix(String fm)
		{
		    Object[] info = (Object[]) m.get(fm);
		    return (String) info[PREFIX_POS];
		}
    
    public static String getURLSuffix(String fm)
    {
	Object[] info = (Object[]) m.get(fm);
	return (String) info[SUFFIX_POS];
    }
    
    public static String getDriverName(String fm)
    {
	Object[] info =  (Object[]) m.get(fm);
	if (info != null)
	    return (String) info[DRIVER_POS];
	else 
	    return null;
    }
    
    public static  boolean isDB2Connection(String fm)
    {
	return (fm.toUpperCase(Locale.ENGLISH).equals("DB2APP") ||
		fm.toUpperCase(Locale.ENGLISH).equals("DB2JCC"));

    }

	public static boolean isNetworkServerConnection(String fm)
	{
		return (fm.toUpperCase(Locale.ENGLISH).startsWith("DERBYNET"));
	}

    public static boolean isClientConnection(String fm)
    {
	return (fm.toUpperCase(Locale.ENGLISH).startsWith("DERBYNET") ||
		fm.toUpperCase(Locale.ENGLISH).equals("DB2JCC"));
    }

	public static boolean isJCCConnection(String fm)
	{
		return fm.toUpperCase(Locale.ENGLISH).equals("DB2JCC") || 
			fm.toUpperCase(Locale.ENGLISH).equals("DERBYNET");
	}

    /**
     * @param fm framework name. database url from properties file
     * @return  
     * altered url (i.e. attributes stripped for DB2 and DerbyNet)
     */

    public static String alterURL(String fm, String url)
    {
	String urlPrefix = "jdbc:splice:";
	String newURLPrefix = getURLPrefix(fm);
	String newURLSuffix = getURLSuffix(fm);
	
	// If we don't have a URL prefix for this framework
	// just return
	if (newURLPrefix == null)
	    return url;

	if (newURLSuffix == null)
	    newURLSuffix = "";
	
	if (url.equals(urlPrefix)) // Replace embedded
	    return newURLPrefix;

	// If this is a DB2 connection we need to strip 
	// the connection attributes
	int attrOffset = url.indexOf(';');
	if (NetServer.isDB2Connection(fm)  &&
		attrOffset != -1)
	    url = url.substring(0,attrOffset);
	
	
	if (url.startsWith(urlPrefix))
	{
	    // replace jdbc:splice: with our url:
	    url = newURLPrefix +
		url.substring(urlPrefix.length()) +
		newURLSuffix;
	}
	else
	{
	    if (! (url.startsWith("jdbc:")))
	    {
		url = newURLPrefix + url + newURLSuffix;
	    }
	}
	return url;
    }
    

}
