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

package com.splicemachine.db.impl.tools.sysinfo;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Locale;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.zip.ZipFile;
import java.util.zip.ZipEntry;
import java.util.Vector;
import java.net.URL;
import java.net.URLDecoder;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.security.CodeSource;

import com.splicemachine.db.iapi.services.info.ProductVersionHolder;

import com.splicemachine.db.iapi.tools.i18n.*;


/**
  <P>
  Sysinfo reports values relevant to the current Derby configuration.

  <P>
  Sysinfo looks for properties files in com.splicemachine.db.info named after
  the genus names in com.splicemachine.db.tools.sysinfo, and gets their location
  using getResource. It also searches the classpath and attempts to load
  the info properties files from the directory or jar locations on the
  classpath, and eliminates any duplicated information. If no files
  are found, or some other exception occurs, the
  value returned will be that set for the key
  SysInfo.failureTag, or be the value "<info unavailable>".

  <P>
  This class can be used to print out system information at the
  command line by issuing the command:
  <PRE>
    java com.splicemachine.db.tools.sysinfo
  </PRE>
  Alternatively, you can use SysInfo within your program to display
  Derby information; a Derby version string is returned by this Java code:
  <PRE>
    new Main().toString();
  </PRE>

 */


public final class Main {

    /**
     * Name of file which contains messages for sysinfo.
     */
    private final static String MESSAGE_FILE =
        "com.splicemachine.db.loc.sysinfoMessages";

    /**
     * Resource for localizing the sysinfo messages.
     *
     * The default LocalizedResource reads messages from the toolsmessages
     * bundle. Create this instance to read messages from sysinfoMessages. Use
     * the locale and codeset specified by db.ui.locale and db.ui.codeset
     * if they are set.
     *
     * Note that this variable must be initialized when the class is loaded in
     * order to work correctly for the API methods that don't call
     * <code>main()</code>.
     */
    private final static LocalizedResource LOCALIZED_RESOURCE =
        new LocalizedResource(null, null, MESSAGE_FILE);

  /**
    Application entry point for SysInfo.   This will print out
    the Derby product information as well as a snapshot of
    the System properties.
  */
  public static void main(String args[]) {
        // adjust the application in accordance with db.ui.locale and db.ui.codeset
        LocalizedResource.getInstance().init();

		LocalizedOutput out;

        //using AppStreamReader(extends InputStreamReader) for conversion into specific codeset

		out = LocalizedResource.OutputWriter();

       // because we're in a static method, we need to
       // get our own instance variable
    parseArgs (args);

    if (cptester)
	getClasspathInfo (args, out);
    else
	getMainInfo (out, setPause);

  } // end of main (String args[])

public static void getMainInfo (java.io.PrintWriter aw, boolean pause) {

    aw.println (javaSep);
    reportJavaInfo (aw);
    aw.println (jbmsSep);
    reportDerby (aw);
    aw.println (sep);


    // Locales info
    try {
      reportLocales (aw);
    }
    catch (Exception e) {

      aw.println (Main.getTextMessage ("SIF01.Q"));
      aw.println (Main.getTextMessage ("SIF01.B"));
    }


    if (pause) {
     pause();
    }

  } // end of getMainInfo (AppStreamWriter aw, boolean printLicense, boolean pause)


  private static boolean setPause = false;

  private static boolean setLicense = false;

  private static boolean cptester = false;

  private static void parseArgs (String args[]) {

    if (args == null) {

      return;
    }


    for (int i = 0; i < args.length; i++) {

      if (args[i].equals ("-pause")) {

        setPause = true;
      }

      if (args[i].equals ("-cp")) {

        cptester=true;
      }

    } // end for

  } // end of parseArgs (String args[])


  /**
    For the benefit of DOS box users, this method waits for input
    before returning
  */
  private static void pause () {

    try {

      System.out.print (Main.getTextMessage ("SIF01.C"));
      BufferedReader br = new BufferedReader (new InputStreamReader (System.in, StandardCharsets.UTF_8));
      br.readLine ();
    }
    catch (IOException ioe) {

      //just return
    }

  } // end of pause ()

  /**
    prints out the jbms info to the specified AppStreamWriter.
    @param localAW the AppStreamWriter to use. If null, System.out is
    used
  */

  private static void reportDerby (java.io.PrintWriter localAW) {

	  String classpath;

      try {
          classpath = (String) AccessController.doPrivileged( new PrivilegedAction()
              {
                  public Object run()
                  {
                      return System.getProperty("java.class.path");
                  }
              }
              );
	  }
	  catch (SecurityException se) {
          localAW.println(
              Main.getTextMessage ("SIF01.U", se.getMessage()));
		  classpath = null;
	  }

      ZipInfoProperties zip[]= Main.getAllInfo (classpath);

      for (ZipInfoProperties zipInfoProperties : zip) {

          String thisInfo = "[" + zipInfoProperties.getLocation() + "] " +
                  zipInfoProperties.getVersionBuildInfo();

          localAW.println(thisInfo);
      }


  } // end of reportDerby

  /**
    Writes out the relevant info about the Java environment to
    the specified AppStreamWriter.

    @param localAW The AppStreamWriter to write info out to. If this is
    null, the info is written to System.out
  */

  private static void reportJavaInfo (java.io.PrintWriter localAW) {

	

    localAW.println (Main.getTextMessage ("SIF02.A",
                                                           getJavaProperty ("java.version")));

    localAW.println (Main.getTextMessage ("SIF02.B",
                                                           getJavaProperty ("java.vendor")));

    localAW.println (Main.getTextMessage ("SIF02.C",
                                                           getJavaProperty ("java.home")));

    localAW.println (Main.getTextMessage ("SIF02.D",
                                                           getJavaProperty ("java.class.path")));

    localAW.println (Main.getTextMessage ("SIF02.E",
                                                           getJavaProperty ("os.name")));

    localAW.println (Main.getTextMessage ("SIF02.F",
                                                           getJavaProperty ("os.arch")));

    localAW.println (Main.getTextMessage ("SIF02.G",
                                                           getJavaProperty ("os.version")));

    localAW.println (Main.getTextMessage ("SIF02.H",
                                                           getJavaProperty ("user.name")));

    localAW.println (Main.getTextMessage ("SIF02.I",
                                                           getJavaProperty ("user.home")));

    localAW.println (Main.getTextMessage ("SIF02.J",
                                                           getJavaProperty ("user.dir")));

	localAW.println("java.specification.name: " + getJavaProperty("java.specification.name"));
	localAW.println("java.specification.version: " + getJavaProperty("java.specification.version"));
	printPropertyIfNotNull(localAW, "java.runtime.version");
	printPropertyIfNotNull(localAW, "java.fullversion");


  } // end of reportJavaInfo

  /**
   * Print property only if not null
   * 
   * @param localAW This is PrintWriter to print to
   * @param property This is system property string
   */
  private static void printPropertyIfNotNull(java.io.PrintWriter localAW, String property) {
    String propertyValue = getJavaProperty(property, true);	

    if (propertyValue != null) {
        localAW.println(property + ": " + propertyValue);
    }
}
  
  /**
   * Return Java properties from java.lang.System. Will catch
   * SecurityExceptions and note them for displaying information.
   * @param whichProperty This is the name of the property
   * 
   * @return getJavaProperty(whichProperty, false) 
   */
  private static String getJavaProperty (final String whichProperty) {
	  return getJavaProperty(whichProperty, false);  
  }
 
  /**
   * Return Java properties from java.lang.System. Will catch
   * SecurityExceptions and note them for displaying information.
   * @param whichProperty This is the name of the property
   * @param nullUnavailable return nothing if no such java property and nullUnavailable is true
   * @return the Java property value or a string capturing a
   * security exception.
   */

  private static String getJavaProperty (final String whichProperty, boolean nullUnavailable) {

    final   String unavailable = nullUnavailable ? null : Main.getTextMessage ("SIF01.H");

    try {
        return (String) AccessController.doPrivileged(new PrivilegedAction()
            {
                public  Object  run()
                {
                    return System.getProperty (whichProperty, unavailable);
                }
            }
            );
    }
    catch (SecurityException se) {

      return Main.getTextMessage ("SIF01.I", se);
    }

  } // end of getJavaProperty (String whichProperty)


    /**
     * wrapper for getCanonicalPath for sysinfo. For sysinfo we just want to print
     * the security exceptions, not throw them if we don't have permmission
     * 
     * @param f file on which to call getCanonicalPath
     * @return f.getCanonicalPath
     * @throws IOException
     */
    private static String getCanonicalPath(final File f) throws IOException {

        try {
            return (String) AccessController
                    .doPrivileged(new PrivilegedExceptionAction() {
                        public Object run() throws IOException {
                            return f.getCanonicalPath();
                        }
                    });
        } catch (PrivilegedActionException pae) {
            throw (IOException) pae.getCause();
        } catch (SecurityException se) {
            return Main.getTextMessage("SIF01.I", se);
        }
    }

  /**
    for use by the main () method
   */

  private final static String sep     = "------------------------------------------------------";
  private final static String javaSep = Main.getTextMessage ("SIF01.L");

  private final static String jbmsSep = Main.getTextMessage ("SIF01.M");

  private final static String licSep  = Main.getTextMessage ("SIF01.N");

  private final static String locSep  = Main.getTextMessage ("SIF01.P");

  private final static String curLoc  = Main.getTextMessage ("SIF01.T");

  /**
    The name of the failure tag in the information file.
    The failure tag's value provides a default value if
    any other properties are missing.
   */
  private final static String failureTag = Main.getTextMessage ("SIF01.J");

  private static void getClasspathInfo (String args[], java.io.PrintWriter aw) {

    Main.useMe (args, aw);
  }




  /**
    Writes out information about the locales with the
    product.

    @param localAW the AppStreamWriter to which the info is written. If this
    value is null, the info is written to System.out

  */
  private static void reportLocales (java.io.PrintWriter localAW) {

    boolean cur_loc = true;

    localAW.println (locSep);

    // Read all the possible locales, and test for each one, if it loads.
    // If so, then read the properties, and print them out.

	Locale[] supportedLocales = Locale.getAvailableLocales();
	String[] stringLocales = new String[supportedLocales.length];
    for (int i = 0; i < supportedLocales.length; i++)
	{
		stringLocales[i] = supportedLocales[i].toString();
	}
	java.util.Arrays.sort(stringLocales);

    Properties p = new Properties ();
    for (int i = 0; i < stringLocales.length; i++) {

      String localeResource =
         "/com/splicemachine/db/info/locale_" + stringLocales[i] + ".properties";
      
      final Properties finalp = p;
      final String finalLocaleResource = localeResource;
     
      try {     
        	InputStream is = (InputStream) AccessController.doPrivileged
            (new PrivilegedAction() {
                  public Object run() {
                      return finalp.getClass().getResourceAsStream (finalLocaleResource);
                  }
              }
           );      
      	
        if (is == null) {
//           localAW.println("resource is null: " + localeResource);
        }
        else {

          try {
			  p.clear();
            p.load (is);
        //Displaying Current Locale
	    if (cur_loc)
		{
	Locale loc = null;
	loc = Locale.getDefault();
        localAW.println(Main.getTextMessage ("SIF01.T") + "  [" + loc.getDisplayLanguage() + "/" +  loc.getDisplayCountry() + " [" + loc + "]]");
		cur_loc = false;
		}

	//Beetle 5079: do not print unlocalized locale names to console, only print locale code.
	String localeName = p.getProperty("derby.locale.external.name");
	localeName = localeName.substring(localeName.indexOf("[")+1);
	localeName = localeName.substring(0,localeName.indexOf("]"));
	
            localAW.println (Main.getTextMessage ("SIF01.R",
                                                                   localeName));


			int major = Integer.parseInt(p.getProperty ("derby.locale.version.major"));
			int minor = Integer.parseInt(p.getProperty ("derby.locale.version.minor"));
			int maint = Integer.parseInt(p.getProperty ("derby.locale.version.maint"));
			String build = p.getProperty ("derby.locale.build.number");

			String lv = ProductVersionHolder.fullVersionString(major, minor, maint, false, build);


            localAW.println (Main.getTextMessage ("SIF01.S", lv));


          }
          catch (IOException ioe) {

            //This case is a bit ugly. If we get an IOException, we return
            //null. Though this correctly reflects that the product is not
            //available for use, it may be confusing to users that we swallow
            //the IO error here.

            localAW.println("Could not get locale properties from : " + is);
          }
        }

      }
      catch (Throwable t) {
        localAW.println ("Could not load resource: " + localeResource);
        localAW.println ("Exception: " + t);
      }

    }


    localAW.println (sep);

  } // end of reportLocales

	/* for arguments, choose from one of:*/
	private static final String EMBEDDED = "embedded";

	/* you can add this if you like*/
	private static final String TOOLS = "tools";

	private static final String NET = "server";
	private static final String CLIENT = "client";

	/* you can add this if you like */

	private static final String MAINUSAGESTRING = "java com.splicemachine.db.tools.sysinfo -cp";

    private static final String USAGESTRINGPARTA = MAINUSAGESTRING + " [ [ "
            + EMBEDDED + " ][ " + NET + " ][ " + CLIENT + "] [ " + TOOLS
            + " ] [";
    private static final String USAGESTRINGPARTB = ".class ] ]";

  static  void useMe(String[] args, java.io.PrintWriter pw) {
	  java.io.PrintWriter localPW = pw;

	    if (localPW == null)
	    {
	        localPW = new java.io.PrintWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8), true);
	    }


      int length = args.length;
	  if (length==1) {

		  try {
			  tryAllClasspaths(localPW);

		  }

		  catch (Throwable t) {

		  }
	  }
	  else {
		  try {
			  trySomeClasspaths(args, localPW);
		  }

		  catch (Throwable t) {

		  }
	  }

  }





	  private static void tryAllClasspaths(java.io.PrintWriter localPW) throws Throwable {
		  localPW.println(Main.getTextMessage("SIF08.B"));
		  localPW.println(Main.getTextMessage("SIF08.C", MAINUSAGESTRING + " args"));
		  StringBuffer successes = new StringBuffer(Main.getTextMessage("SIF08.D")+ crLf());
		  StringBuffer failures = new StringBuffer(crLf() + Main.getTextMessage("SIF08.E") + crLf());
		  tryCoreClasspath(successes, failures);
		  tryNetClasspath(successes, failures);
		  tryClientClasspath(successes, failures);
		  tryUtilsClasspath(successes, failures);
		  localPW.println(successes.toString());
		  if (!failures.toString().equals(crLf() + Main.getTextMessage("SIF08.E") + crLf())) {
			  localPW.println(failures.toString());
		  }
		  else {

			  localPW.println(Main.getTextMessage("SIF08.F"));
		  }
		  localPW.flush();
	  }

	private static void trySomeClasspaths(String[] args, java.io.PrintWriter localPW) throws Throwable {

		boolean seenArg = false;
		StringBuffer successes = new StringBuffer(Main.getTextMessage("SIF08.D")+ crLf());
		StringBuffer failures = new StringBuffer(crLf() + Main.getTextMessage("SIF08.E") + crLf());

		if (argumentsContain(args, EMBEDDED))
		{

			tryCoreClasspath(successes, failures);
			seenArg =true;

		}
		if (argumentsContain(args,NET)) {
		  tryNetClasspath(successes, failures);
			seenArg =true;

		}
		if (argumentsContain(args,CLIENT)) {
		  tryClientClasspath(successes, failures);
			seenArg =true;

		}

		if (argumentsContain(args,TOOLS) || argumentsContain(args,"utils")) {
		  tryUtilsClasspath(successes, failures);
			seenArg =true;

		}


		String userclass = argumentMatches(args, ".class");
		if (!userclass.isEmpty()) {
			tryMyClasspath(argumentMatches(args, ".class"), Main.getTextMessage("SIF08.H", userclass), successes, failures);
			seenArg =true;
		}

		if (seenArg)
		{

			localPW.println(successes.toString());
			if (!failures.toString().equals(crLf() + Main.getTextMessage("SIF08.E") + crLf())) {
				localPW.println(failures.toString());
			}
			else {

				localPW.println(Main.getTextMessage("SIF08.F"));
			}
		}
		else
		{
			localPW.println(Main.getTextMessage("SIF08.A", USAGESTRINGPARTA, USAGESTRINGPARTB));
		}
		localPW.flush();

	}

	private static void tryCoreClasspath(StringBuffer successes, StringBuffer failures) {
		tryMyClasspath("com.splicemachine.db.database.Database", Main.getTextMessage("SIF08.J","derby.jar" ), successes, failures);
	}
	private static void tryNetClasspath(StringBuffer successes, StringBuffer failures) {
		tryMyClasspath("com.splicemachine.db.database.Database", Main.getTextMessage("SIF08.J","derby.jar" ), successes, failures);
		tryMyClasspath("com.splicemachine.db.drda.NetworkServerControl", Main.getTextMessage("SIF08.I", "derbynet.jar"), successes, failures);
	}
	private static void tryClientClasspath(StringBuffer successes, StringBuffer failures) {
		tryMyClasspath("com.splicemachine.db.jdbc.ClientDriver", Main.getTextMessage("SIF08.L", "derbyclient.jar"), successes, failures);
	}

	private static void tryUtilsClasspath(StringBuffer successes, StringBuffer failures) {
		tryMyClasspath("com.splicemachine.db.tools.ij", Main.getTextMessage("SIF08.Q", "derbytools.jar"), successes, failures);
	}

	private static void tryMyClasspath(String cn, String library, StringBuffer successes, StringBuffer failures) {

		try {
			Class c = Class.forName(cn);
                        String loc = getFileWhichLoadedClass(c);
                        successes.append(found(cn, library, loc));
		}

		catch (Throwable t) {

			failures.append(notFound(cn, library));

		}


	}

	private static void tryAsResource(String cn, String library, StringBuffer successes, StringBuffer failures) {

		try {
			java.io.InputStream in = cn.getClass().getResourceAsStream(cn);
			in.close();
                        String loc = getFileWhichLoadedClass(cn.getClass());
			successes.append(found(cn, library, loc));
		}

		catch (Throwable t) {
			failures.append(notFound(cn, library));

		}

	}

	private static String found(String cn, String library, String loc) {
		StringBuilder temp = new StringBuilder(crLf());
		temp.append("   " + library);
		temp.append(crLf());
                if (loc != null)
                    temp.append("   ").append(loc).append(crLf());
		temp.append(crLf());
		return temp.toString();
	}
	private static String notFound(String cn, String library) {

		StringBuilder temp = new StringBuilder(crLf());
		temp.append("   " + library);
		temp.append(crLf());
		temp.append("    " + Main.getTextMessage("SIF08.U", cn));
		temp.append(crLf());
		temp.append(crLf());
		return temp.toString();
	}

	private static String crLf() {
		return System.getProperty("line.separator");
	}

	private static String lookForMainArg(String[] args, java.io.PrintWriter localPW)
	{
		int length=args.length;
		String[] legalargs = new String[1];
		legalargs[0] = EMBEDDED;

		int argsfound = 0;
		String whichargument="";

		for (int i = 0; i < length; i++) {

			for (int j=0; j < legalargs.length; j++) {
				if (args[i].toUpperCase(java.util.Locale.ENGLISH).equals(legalargs[j].toUpperCase(java.util.Locale.ENGLISH))) {
					argsfound++;
					whichargument=legalargs[j];
				}
			}
		}
		if (argsfound > 1 || argsfound < 1) {
            localPW.println(Main.getTextMessage("SIF08.A", USAGESTRINGPARTA, USAGESTRINGPARTB));
			return "";
		}
		return whichargument;
	}

	private static boolean argumentsContain(String[] args, String s) {
		for (int i = 0; i < args.length; i++) {
			if (args[i].equalsIgnoreCase(s))
				return true;
		}
		return false;

	}

	private static String argumentMatches(String[] args, String ss) {
	    String userclass = "";
		int length = args.length;
		for (int i = 0; i < length; i++) {
			if (args[i].endsWith(ss)) {
				userclass = args[i].substring(0,args[i].length()-6) ;

			}

		}
		return userclass;
	}

	/*
	** Code related to loading info fromjar files.
	*/

    private static final String infoNames[] = {

                                    "com/splicemachine/db/info/" +
                                    com.splicemachine.db.iapi.services.info.ProductGenusNames.DBMS +
                                    ".properties",


                                    "com/splicemachine/db/info/" +
                                    com.splicemachine.db.iapi.services.info.ProductGenusNames.TOOLS +
                                    ".properties",

                                    "com/splicemachine/db/info/" +
                                    com.splicemachine.db.iapi.services.info.ProductGenusNames.NET +
                                    ".properties",

                                   "com/splicemachine/db/info/" +
                                    com.splicemachine.db.iapi.services.info.ProductGenusNames.DNC +
                                    ".properties"
                                };

    /**
     *  Get all the info we can obtain from the local execution context
     *  as to the availability of the Derby classes by attempting to load
     *  the info files with loadZipFromResource() and checking classpath
     *  locations with checkForInfo if the classpath is accessible.
     *
     *  @param classpath the classpath, or null if not accessible
     *  @return an array of ZipInfoProperties with the locations of the located
     *          resources
     *  @see #loadZipFromResource()
     *  @see #checkForInfo(String)
     */
    public static ZipInfoProperties[] getAllInfo(String classpath)
    {
        ZipInfoProperties zips[] = loadZipFromResource();

        // No info properties files found, but here we are in sysinfo.
        // Avoid an NPE in mergeZips by creating a ZipInfoProperties array
        // with the location of the sysinfo that is currently executing.
        if (zips == null)
        {
            zips = new ZipInfoProperties[1];
            ZipInfoProperties zip = new ZipInfoProperties(ProductVersionHolder.getProductVersionHolderFromMyEnv(com.splicemachine.db.tools.sysinfo.TOOLS));
            zip.setLocation(getFileWhichLoadedClass(Main.class));
            zips[0] = zip;
        }

        try
        {
			if (classpath != null) {
				String cp [] = parseClasspath(classpath);
				Vector v = new Vector();
				for (int i = 0; i < cp.length; i++)
				{
					ZipInfoProperties zip = null;
					try {
						zip = checkForInfo(cp[i]);
					}
					catch (SecurityException se)
					{
						zip = new ZipInfoProperties(null);
						zip.setLocation(
							Main.getTextMessage ("SIF03.C", se.getMessage()));
					}
					if (zip != null)
					{
						v.addElement(zip);
					}
				}
				if (!v.isEmpty())
				{
					ZipInfoProperties cpzips[] = new ZipInfoProperties[v.size()];
					v.copyInto(cpzips);
					return mergeZips(zips, cpzips);
				}
			}
            return mergeZips(zips, null);

        }
        catch (SecurityException se)
        {
            ZipInfoProperties zip[] = new ZipInfoProperties[1];
            zip[0] = new ZipInfoProperties(null);
            zip[0].setLocation(
					Main.getTextMessage ("SIF03.C", se.getMessage()));
            return zip;
        }
    }

    /**
     *  Attempt to load the info properties files specified in infoNames[i]
     *  using getResourceAsStream(). If none are able to be loaded, return
     *  a null array.
     *
     *  @return An array of ZipInfoProperties with the locations from which
     *          the info properties files were loaded.
     *  @see #infoNames
     */
    private static ZipInfoProperties [] loadZipFromResource()
    {
		java.util.ArrayList al = new java.util.ArrayList();

        for (int i = 0; i < infoNames.length; i++)
        {
            final String resource = "/" + infoNames[i];

            InputStream is = (InputStream) AccessController.doPrivileged
            (new PrivilegedAction() {
                public Object run() {
                    return Main.class.getResourceAsStream(resource);
                    }
                }
            );         

			if (is == null)
				continue;

			ZipInfoProperties ze = new ZipInfoProperties(ProductVersionHolder.getProductVersionHolderFromMyEnv(is));
 
                        // get the real location of the info file
                        URL locUrl = (URL) AccessController.doPrivileged
                        (new PrivilegedAction() {
                            public Object run() {
                                return Main.class.getResource(resource);
                            }
                        });

			ze.setLocation(formatURL(locUrl));

			al.add(ze);
        }

        if (al.isEmpty())
        {
            return null;
        }

        ZipInfoProperties[] zip = new ZipInfoProperties[al.size()];

		al.toArray(zip);

        return zip;
    }

    /**
     *  Split the classpath into separate elements.
     *
     *  @param cp the classpath, if accessible.
     *  @return a String array with the individual classpath elements.
     */
    private static String [] parseClasspath(String cp)
    {
        StringTokenizer st = new StringTokenizer(cp, File.pathSeparator);
        int count = st.countTokens();
        if (count == 0)
        {
            return null;
        }

        String vals[] = new String[count];
        for (int i =0; i < count; i++)
        {
            vals[i] = st.nextToken();
        }
        return vals;
    }

    /**
     *  Given an individual element of the element of the classpath, call
     *  checkDirectory() if the element is a directory or checkFile()
     *  if the element is a file.
     *
     *  @param cpEntry the classpath element
     *  @return a ZipInfoProperties if an info properties file is found.
     */
    private static ZipInfoProperties checkForInfo(final String cpEntry)
    {
        return (ZipInfoProperties) AccessController.doPrivileged( new PrivilegedAction()
            {
                public Object run()
                {
                    File f = new File(cpEntry);
                    if ( ! f.exists())
                    {
                        return null;
                    }

                    if (f.isDirectory())
                    {
                        return checkDirectory(cpEntry);
                    }

                    if (f.isFile())
                    {
                        return checkFile(cpEntry);
                    }
                    return null;
                }
            }
            );
        
    }

    /**
     *  Check a given directory for the presence of an info properties file in
     *  com/splicemachine/db/info inside the directory.
     *
     *  @param dirname the directory to check as a String
     *  @return a ZipInfoProperties if a file is found, otherwise null.
     */
    private static ZipInfoProperties checkDirectory(String dirname)
    {
        boolean foundOne = false;
        File f = null;
        for (int i = 0; i < infoNames.length; i++)
        {
            String localSysinfo = infoNames[i].replace('/', File.separatorChar);
            f = new File(dirname, localSysinfo);
            if (f.exists())
            {
                foundOne = true;
                break;
            }
        }

        if (!foundOne || (f == null))
        {
            return null;
        }

        try
        {
            InputStream bis = new FileInputStream(f);

            ZipInfoProperties zip = new ZipInfoProperties(ProductVersionHolder.getProductVersionHolderFromMyEnv(bis));
            zip.setLocation(getCanonicalPath(new File(dirname)).replace('/', File.separatorChar));
            return zip;
        }
        catch (IOException ioe)
        {
            return null;
        }

    }

    /**
     * Check inside a jar file for the presence of a Derby info properties file.
     * 
     * @param filename the jar file to check
     * @return ZipInfoProperties with the jar file set as the location or null
     *  if not found, or an IOException occurs.
     */
    private static ZipInfoProperties checkFile(String filename)
    {
        // try to create a ZipFile from the file name
        try(ZipFile zf = new ZipFile(filename))
        {
            // try to get a ZipEntry from the ZipFile
            ZipEntry thisEntry = null;
            for (String infoName : infoNames) {
                thisEntry = zf.getEntry(infoName);
                if (thisEntry != null) {
                    break;
                }
            }
            if (thisEntry == null)
            {
                return null;
            }

            ZipInfoProperties zip;
            try(InputStream bis = zf.getInputStream(thisEntry))
            {
                zip = new ZipInfoProperties(ProductVersionHolder.getProductVersionHolderFromMyEnv(bis));
                zip.setLocation(getCanonicalPath(new File(filename)).replace('/', File.separatorChar));
            }
            return zip; // can be null
        }
        catch (IOException ioe)
        {
            //guess not
            return null;
        }
    }

	/*
	** Message handling
	*/

	public static String getTextMessage(String msgId) {
		return getCompleteMessage(msgId, new Object[0]);
	}
	public static String getTextMessage(String msgId, Object a1) {

		return getCompleteMessage(msgId, new Object[] {a1});
	}
	public static String getTextMessage(String msgId, Object a1, Object a2) {
		return getCompleteMessage(msgId, new Object[] {a1, a2});
	}
	public static String getTextMessage(String msgId, Object a1, Object a2, Object a3) {
		return getCompleteMessage(msgId, new Object[] {a1, a2, a3});
	}
	public static String getTextMessage(String msgId, Object a1, Object a2, Object a3, Object a4) {
		return getCompleteMessage(msgId, new Object[] {a1, a2, a3, a4});
	}

	/**
	 */
	public static String getCompleteMessage(String msgId, Object[] arguments) {
        return LOCALIZED_RESOURCE.getTextMessage(msgId, arguments);
	}

    /**
     * Given a loaded class, this
     * routine asks the class's class loader for information about where the
     * class was loaded from. Typically, this is a file, which might be
     * either a class file or a jar file. The routine figures that out, and
     * returns the name of the file. If it can't figure it out, it returns null
     */
    private static String getFileWhichLoadedClass(final Class cls)
    {
         return (String)AccessController.doPrivileged( new PrivilegedAction()
        {
            public Object run()
            {
                CodeSource cs = null;
                try {
                    cs = cls.getProtectionDomain().getCodeSource ();
                }
                catch (SecurityException se) {
                    return Main.getTextMessage(
                        "SIF01.V", cls.getName(), se.getMessage());
                }
 
                if ( cs == null )
                    return null;        
     
                URL result = cs.getLocation ();

                try {
                    // DERBY-4806 Should use UTF-8 according to
                    // http://www.w3.org/TR/html40/appendix/notes.html#non-ascii-chars
                    // to get the string of the file name
                    return URLDecoder.decode(result.toString(), "UTF-8");
                } catch (UnsupportedEncodingException e) {
                    // All JVMs are required to support UTF-8.
                    return e.getMessage();
                }
            }
        });
    }

    /**
     *  <P>
     *  Merge and flatten two arrays of ZipInfoProperties, removing any 
     *  duplicates. There may be duplicates in the arrays because
     *  loadZipFromResource may find all the properties files in the same
     *  location, such as when loading from compiled source instead of
     *  packaged jars. Also, a poorly constructed classpath may contain
     *  duplicate entries that each contain the Derby classes, and we
     *  need only report the first of each such instances found.
     *  <P>
     *  The second array may be null if the classpath was empty, in which
     *  case we still remove the duplicates from the first array and return 
     *  the shortened array.
     *
     *  @param zip1 the first array from loadZipWithResource
     *  @param zip2 the second array from analyzing the classpath
     *  @return the merged array
     */
    private static ZipInfoProperties[] mergeZips(ZipInfoProperties[] zip1,
                                                 ZipInfoProperties[] zip2)
    {
        Vector v = new Vector();
        boolean foundDup = false;
  
        // remove duplicates from first array
        for (int i = 0; i < zip1.length; i++)
        {
            if (zip1[i] != null && zip1.length > 1)
            {
                for (int j = i + 1; j < zip1.length; j++)
                {
                    if (zip1[i].getLocation().equals(zip1[j].getLocation()))
                    zip1[j] = null;
                }
            }
            if (zip1[i] != null)
              v.addElement(zip1[i]);
        }
  
        // if provided a second array, remove any locations in second array
        // still in first array.
        if (zip2 != null)
        {
          for (int j = 0; j < zip2.length; j++)
          {
            for (int k = 0; k < v.size(); k++)
            {
                ZipInfoProperties z = (ZipInfoProperties)v.get(k);
                if (zip2[j].getLocation().equals(z.getLocation()))
                  foundDup = true;
            }
            if (!foundDup)
            {
                v.addElement(zip2[j]);
            }
            foundDup = false;
          }
        }
  
        ZipInfoProperties[] merged = new ZipInfoProperties[v.size()];
        v.copyInto(merged);
        return merged;
    }

    /**
     *  Strip a given URL down to the filename. The URL will be a jarfile or
     *  directory containing a Derby info properties file. Return the canonical
     *  path for the filename, with the path separators normalized.
     */
    private static String formatURL(URL loc)
    {
        String filename;
        try {
            // Should use UTF-8 according to
            // http://www.w3.org/TR/html40/appendix/notes.html#non-ascii-chars
            filename = URLDecoder.decode(loc.toString(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            // All JVMs are required to support UTF-8.
            return null;
        }

        if (filename.startsWith("jar:")) { filename = filename.substring(4); }
        if (filename.startsWith("file:")) { filename = filename.substring(5); }
        if (filename.contains("!")) { filename = filename.substring(0, filename.indexOf("!")); }
        if (filename.contains("/com/splicemachine/db")) {
            filename = filename.substring(0, filename.indexOf("/com/splicemachine/db"));
        }
        if (filename.charAt(0) == '/' && 
            Character.isLetter(filename.charAt(1)) &&
            filename.charAt(2) == ':' &&
            filename.charAt(2) == '/') { filename = filename.substring(1); }

        String result = ""; 
        try {
            result = getCanonicalPath(new File(filename)).replace('/', File.separatorChar);
        } catch (IOException e) {
            result = e.getMessage();
        }
        return result;
    }

} // end of class Main

