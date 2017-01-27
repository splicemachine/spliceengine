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

import java.util.Enumeration;
import java.util.Properties;
import java.util.Vector;
import java.util.StringTokenizer;
import java.io.File;
import java.io.IOException;

import com.splicemachine.dbTesting.junit.SecurityManagerSetup;


/**
  <p>This class provides the interface and mechanism
  for plugging VMs into the system.  Typically
  you only need to add a new implementation if your
  supported attributes or command line building are
  different from those that exist.

  <p>this class has fields for all options that a JDK VM can take,
  that is the reference point for all others.  Note some VMs (like jview)
  don't take all options and will ignore them (like -mx).  Defining
  the system property "verbose" to 1 will give you warnings for ignored
  properties in a properly implemented subclass.

  <p> here is the canonical output from java -help for options we take:
  <pre>
    -noasyncgc        don't allow asynchronous garbage collection
    -verbosegc        print a message when garbage collection occurs
    -noclassgc        disable class garbage collection
    -ss<number>       set the maximum native stack size for any thread
    -oss<number>      set the maximum Java stack size for any thread
    -ms<number>       set the initial Java heap size
    -mx<number>       set the maximum Java heap size
    -classpath <directories separated by semicolons>
                      list directories in which to look for classes
    -prof[:<file>]    output profiling data to .\java.prof or .\<file>
    -verify           verify all classes when read in
    -noverify         do not verify any class
    -nojit            turn off the jit
    -Dprop=name       define property; can be specified more than once
  </pre>

 */


public abstract class jvm {

    // they all take their defaults as the initial value.
    // -1, null, and false all will mean we won't include them
    // in the command line.

    // flags             just take the whole string of flags as is
    public String flags = null;
    // -noasyncgc        don't allow asynchronous garbage collection
    public boolean noasyncgc = false;
    // -verbosegc        print a message when garbage collection occurs
    public boolean verbosegc = false;
    // -noclassgc        disable class garbage collection
    public boolean noclassgc = false;
    // -ss<number>       set the maximum native stack size for any thread
    public long ss = -1;
    // -oss<number>      set the maximum Java stack size for any thread
    public long oss = -1;
    // -ms<number>       set the initial Java heap size
    public long ms = -1;
    // -mx<number>       set the maximum Java heap size
    public long mx = -1;
    // -classpath <directories separated by semicolons>
    //                   list directories in which to look for classes
    public String classpath = null;
    // -prof[:<file>]    output profiling data to .\java.prof or .\<file>
    public String prof = null;
    // -verify           verify all classes when read in
    //                   (remote verification is the default)
    public boolean verify = false;
    // -noverify         do not verify any class
    //                   (remote verification is the default)
    public boolean noverify = false;
    // -nojit            turn off the jit
    public boolean nojit = false;
    // -Dprop=name       define property; can be specified more than once
    public Vector D = null;
    // java cmd (java, java_g)
    public String javaCmd = "java";
    // major and minor version
    public String majorVersion = "";
    public String minorVersion = "";
    public int imajor = 0;
    public int iminor = 0;
    String hostName;

	// security defaults relative to WS
	// not used if jvmargs serverCodeBase are set
	private static String DEFAULT_POLICY="util/derby_tests.policy";
	private static String DEFAULT_CODEBASE="/classes";

    // constructors
    public jvm() { }

    public jvm(boolean noasyncgc, boolean verbosegc, boolean noclassgc,
    long ss, long oss, long ms, long mx, String classpath, String prof,
    boolean verify, boolean noverify, boolean nojit, Vector D) {
        this.noasyncgc=noasyncgc;
        this.noclassgc=noclassgc;
        this.verbosegc=verbosegc;
        this.ss=ss;
        this.oss=oss;
        this.ms=ms;
        this.mx=mx;
        this.classpath=classpath;
        this.prof=prof;
        this.verify=verify;
        this.noverify=noverify;
        this.nojit=nojit;
        this.D=D;
    }
    // more typical use:
    public jvm(String classpath, Vector D) {
        this.classpath=classpath;
        this.D=D;
    }
    // more typical use:
    public jvm(long ms, long mx, String classpath, Vector D) {
        this.ms=ms;
        this.mx=mx;
        this.classpath=classpath;
        this.D=D;
    }

	/**
       return the property definition introducer, with a space if a
       separator is needed.
     */
    public abstract String getDintro();
	public abstract String getName();
    public void setNoasyncgc(boolean noasyncgc) { this.noasyncgc=noasyncgc; }
    public void setNoclassgc(boolean noclassgc) { this.noclassgc=noclassgc; }
    public void setVerbosegc(boolean verbosegc) { this.verbosegc=verbosegc; }
    public void setSs(long ss) { this.ss=ss; }
    public void setOss(long oss) { this.oss=oss; }
    public void setMs(long ms) { this.ms = ms; }
    public void setMx(long mx) { this.mx = mx; }
    public void setClasspath(String classpath) { this.classpath = classpath; }
    public void setProf(String prof) { this.prof=prof; }
    public void setVerify(boolean verify) { this.verify=verify; }
    public void setNoverify(boolean noverify) { this.noverify=noverify; }
    public void setNojit(boolean nojit) { this.nojit=nojit; }
    public void setD(Vector D) { this.D = D; }
    public void setFlags(String flags) { this.flags = flags; }
    public void setJavaCmd(String jcmd) { this.javaCmd = jcmd; }

	
    public Vector getCommandLine()
    {
        Vector v = new Vector();
        v.addElement(javaCmd);
        v.addElement("-Duser.language=en");
        v.addElement("-Duser.country=US");
        if ( (flags != null) && (flags.length()>0) )
        {
            StringTokenizer st = new StringTokenizer(flags);
            while (st.hasMoreTokens())
            {
                v.addElement(st.nextToken());
            }
        }
        return v;
    }

    // implementation, used by subclasses only
    int verboselevel = -1;
    public void warn(String msg) {
      if (verboselevel == -1) {
         try {
           verboselevel = Integer.parseInt((String)(System.getProperty("verbose")));
         } catch (Exception e) {
           verboselevel = 0;
         }
      }
      if (verboselevel >0)
          System.out.println("jvm: "+msg);
    }

    // utility for locating a jvm.
    /**
        pass in class name for JVM.  If we can't find it, try
	also com.splicemachine.dbTesting.functionTests.harness.<jvmName>
     */
    public static jvm getJvm(String jvmName) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
	jvm result = null;
        try {
		result = (jvm)Class.forName(jvmName).newInstance();
        } catch (ClassNotFoundException e) {
		result = (jvm)Class.forName("com.splicemachine.dbTesting.functionTests.harness."+jvmName).newInstance();
        }
        return result;
    }

	/**
	  Get the current JVM using the normal test harness rules for finding
	  a JVM.
	  <OL>
	  <LI> If the sytem property 'jvm' use this name.
	  <LI> else if the java version starts with 1.2 use
	       "jdk12".
	  <LI> else use "currentjvm".	
	  */
	public static jvm getCurrentJvm() throws Exception
	{
		String jvmName = System.getProperty("jvm");
		if ( (jvmName == null) || (jvmName.length()==0) )
		{
			String javaVersion = System.getProperty("java.version");
		    if (javaVersion.startsWith("1.2"))
		        jvmName = "jdk12";
		    else
		        jvmName = "currentjvm";
		}
		return getJvm(jvmName);
	}

    /**
      Return the major version number
    */
    public int getMajorVersion()
    {
        return imajor;
    }
    
    /**
      Return the major version number
    */
    public int getMinorVersion()
    {
        return iminor;
    }
    
	/**
	  Get the current JVM using the normal test harness rules for finding
	  a JVM.
	  */
	public void setVersion() throws Exception
	{
		// check for jdk12 or higher
	    String javaVersion = System.getProperty("java.version");
		int i = javaVersion.indexOf('.');
		int j = javaVersion.indexOf('.', i+1);
		majorVersion = javaVersion.substring(0, i);
		minorVersion = javaVersion.substring(i+1, j);
		Integer minor = new Integer(minorVersion);
		iminor = minor.intValue();
		Integer major = new Integer(majorVersion);
		imajor = major.intValue();
		
		String jvmName = System.getProperty("jvm");
		
		if ( (jvmName == null) || (jvmName.length()==0) )
		{
		    if (iminor < 2)
		        jvmName = "currentjvm";
		    else
		        jvmName = "jdk" + majorVersion + minorVersion;
		}
	}
	
	/** Find $WS based on the assumption that JAVA_HOME is $WS/<jvm_name>
	 * or $WS/<jvm_name>/jre
	 * @return path of $WS
	 */
	protected static String guessWSHome()
	{
		String wshome=""; 
		String jhome = System.getProperty("java.home");
		String sep = System.getProperty("file.separator");
		// need to strip off the java directory  assuming it's something
		// like ibm14/jre or ibm14
		int havejre=jhome.indexOf(sep + "jre");
		if (havejre > 0)
		{
			wshome = jhome.substring(0,jhome.indexOf(sep + "jre"));
			if (wshome.lastIndexOf(sep) >= 0)
				wshome = wshome.substring(0,wshome.lastIndexOf(sep));
		}
		{
			if (jhome.lastIndexOf(sep) >= 0)
				wshome = jhome.substring(0,jhome.lastIndexOf(sep));	
		}
		return wshome;
	}

	public static String findCodeBase(boolean[] isJar)
	{
		throw new UnsupportedOperationException("splice");
	}
	
	/**
	 * set up security properties for server command line.
	 */
	protected void setSecurityProps() throws java.io.IOException, ClassNotFoundException
	{		
		// gd again D = jvm.getSecurityProps(D);
		
	}
	
	static Vector getSecurityProps(Vector D) throws ClassNotFoundException, IOException
	{
		if (D == null)
			D = new Vector();
		
		String userDir = System.getProperty("user.dir");
		String policyFile = userDir + baseName(DEFAULT_POLICY);

		String serverCodeBase = System.getProperty("serverCodeBase");
		boolean[] isJar = new boolean[1];
		if (serverCodeBase == null)
			serverCodeBase = findCodeBase(isJar);
   
        
		if (serverCodeBase == null)
		{
			String ws = guessWSHome();
			serverCodeBase = ws + DEFAULT_CODEBASE;
                 
		}
		
		File pf = new File(policyFile);
		File cb = new File(serverCodeBase);

		if (!pf.exists())
		{
			System.out.println("WARNING: Running without Security manager." +
							   "policy File (" + policyFile + 
							   ") or serverCodeBase(" +  serverCodeBase + 
							   ") not available");
		return D;
		}
		
		D.addElement("java.security.manager");
		D.addElement("java.security.policy=" + pf.getAbsolutePath());
 
        Properties jusetup =
            SecurityManagerSetup.getPolicyFilePropertiesForOldHarness();
        // Take the definitions from the way JUnit tests
        // set them up. This then supports the jar files being
        // in different locations.
        for (Enumeration p = jusetup.keys(); p.hasMoreElements(); )
        {
            String key = (String) p.nextElement();
            D.addElement(key + "=" + jusetup.getProperty(key));
        }
		

		// file path to the codebase
		D.addElement("derbyTesting.codedir=" + cb.getAbsolutePath());
		String hostName = (System.getProperty("hostName"));
		if (hostName == null)
			hostName="localhost";
		D.addElement("derbyTesting.serverhost=" + hostName);
		// in the case of testing with a remote host, this is irrelevant, 
		// when testing 'normal' it is also localhost:
		D.addElement("derbyTesting.clienthost=" + hostName);	 	
		
		return D;
		
	}

	/** Get the base file name from a resource name string
	 * @param resourceName (e.g. /org/apache/derbyTesting/functionTests/util/derby_tests.policy)
	 * @return short name (e.g. derby_tests.policy)
	 */
	private static String baseName(String resourceName)
	{
	  
		return resourceName.substring(resourceName.lastIndexOf("/"),resourceName.length());
	}
}
