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

package com.splicemachine.derbyBuild;

//import com.splicemachine.db.iapi.services.classfile.ClassInvestigator;
//
import java.io.*;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * A tool that generates a list of required classes from a set of properties files. The value of any property within a
 * property file that starts with 'derby.module.' is taken as a class name.
 *
 * That class name and all the clases it requires are listed to
 * System.out, to facilitate building a zip file. Classes that
 * start with 'java.' or 'javax.' are not listed and are not
 * checked for dependent classes.
 * <P>
 * If the class name starts with 'com.ibm.db2j.' then a messages.properties
 * file is searched for corresponding to that class, if one exists then
 * is is added to the list of files printed.
 * <P>
 * The search path for the classes is $CLASSPATH
 * <P>
 * If the system property cloudscapeOnly is set to true then only classes
 * and message.properties files are listed that start with com.ibm.db2j.
 * <P>
 * The output for each class or properties file is a relative file
 * name that uses '/' as the file separator. e.g.
 *
 * com/ibm/db2j/core/Setup.class
 *
 * <P>
 * The output order of the classes & files is random.
 * <P>
 *
 *
 * Usage: java [-DignoreWebLogic=true] [-Dverbose=true] [-DcloudscapeOnly=true] [-DruntimeOnly=true]
 * [-Ddb2jtools=true]
 * [-DportingOnly=true] [-Doutputfile=<filename>] com.splicemachine.derbyBuild.classlister
 * property_file [ property_file ... ]
 */

public class classlister {

    protected String[] sets;
    protected Hashtable<String, String> foundClasses;
    //protected ClassUtilitiesFactory cuf;

    protected boolean cloudscapeOnly = false;
    protected boolean portingOnly = false;
    protected boolean ignoreWebLogic = false;
    protected boolean verbose = false;
    protected boolean skipJava = true;
    protected boolean skipJavax = true;
    protected boolean skipOrg = true;
    protected boolean skipInformix = true;
    protected boolean skipDB2 = true;
    protected boolean skipDB2e = true;
    protected boolean skipSun = true;
    protected boolean showAll = false;
    protected boolean keepRolling = false;
    protected boolean showOne = false;
    protected Hashtable<String, Hashtable<String, String>> masterClassList =
            new Hashtable<String, Hashtable<String, String>>();
    protected String classpath[] = null;
    protected String outputfile;
    protected Hashtable<String, Object> classpathHash;
    protected int indent = 0;
    protected int errorCount = 0;
    protected PrintWriter pwOut;
    protected PrintStream psOut;

    protected boolean db2jtools;
    protected boolean db2jdrda;

    protected boolean keepDependencyHistory;

    protected static final String[] propFiles = {
            "messages.properties",
            "instructions.properties",
            "metadata.properties"
    };

    public static void main(String args[]) throws IOException {

        classlister me = new classlister();

        me.sets = args;

        me.run();
        if (me.errorCount > 0) {
            System.out.println(me.errorCount + " errors encountered.");
            System.exit(1);
        }
    }

    public classlister() {
        cloudscapeOnly = Boolean.getBoolean("cloudscapeOnly");
        portingOnly = Boolean.getBoolean("portingOnly");
        ignoreWebLogic = Boolean.getBoolean("ignoreWebLogic");
        verbose = Boolean.getBoolean("verbose");
        skipJava = !Boolean.getBoolean("doJava");
        skipJavax = !Boolean.getBoolean("doJavax");
        skipOrg = !Boolean.getBoolean("doOrg");
        showAll = Boolean.getBoolean("showAll");
        showOne = Boolean.getBoolean("showOne");
        keepRolling = Boolean.getBoolean("keepRolling");
        outputfile = System.getProperty("outputfile");
        db2jtools = Boolean.getBoolean("db2jtools");
        db2jdrda = Boolean.getBoolean("db2jdrda");

        keepDependencyHistory = showOne || showAll;
    }

    public void run() {
        //System.out.println("outputfile: " + outputfile);
        try {
            File outFile = new File(outputfile);
            pwOut = new PrintWriter(new BufferedWriter
                    (new FileWriter(outFile.getPath()), 10000), true);
        } catch (IOException ioe) {
            System.out.println(ioe);
            System.exit(1);
        }

        loadClasspath();
        //cuf = new ModifyClasses();

        foundClasses = new Hashtable<String, String>(3000, 0.8f);

        for (int i = 0; i < sets.length; i++) {

            // If a set name ends in '.class' then take it as a class
            // name of the form com.acme.foo.MyClass.class.
            try {

                String s = sets[i];

                if (s.endsWith(".class")) {

                    findDependencies(s.substring(0, s.length() - 6));
                } else {

                    FileInputStream fis = new FileInputStream(s);

                    Properties pset = new Properties();

                    pset.load(fis);

                    findClasses(pset);
                }

            } catch (IOException ioe) {
                System.err.println(ioe.toString());
                System.exit(1);
            }
        }
        if (pwOut == null) {
            System.out.println("Need to specify an outputfile");
            System.exit(1);
        }
        for (Enumeration e = foundClasses.keys(); e.hasMoreElements(); ) {
            String name = (String) e.nextElement();
            String type = (String) foundClasses.get(name);
            if (type.equals("class")) {
                if (ignoreWebLogic) {
                    if (name.startsWith("weblogic")) {
                        continue;
                    }
                }


                if (isCloudscapeCode(name)) {

                    if (name.startsWith("com.ibm.db2j.porting.")) {
                        if (cloudscapeOnly)
                            continue;
                    } else {
                        if (portingOnly)
                            continue;
                    }

                } else {
                    if (cloudscapeOnly || portingOnly)
                        continue;
                }
                pwOut.println(name.replace('.', '/') + ".class");
            } else {
                // is a file name
                if (name.startsWith("com/ibm/db2j/")) {
                    if (portingOnly) {
                        continue;
                    }
                } else {
                    if (cloudscapeOnly || portingOnly)
                        continue;
                }

                pwOut.println(name);
            }
        }
        if (showAll) {
            showAllItems();
        }
        if (showOne) {
            showAllItemsOneLevel();
        }
    }


    protected void findClasses(Properties pset) throws IOException {

        for (Enumeration e = pset.propertyNames(); e.hasMoreElements(); ) {
            String key = (String) e.nextElement();
            if (key.startsWith("derby.module.")) {
                if (verbose) {
                    pwOut.println(pset.getProperty(key) + " needs ");
                }
                findDependencies(pset.getProperty(key));
            }
        }
    }

    protected void loadClasspath() {
        classpathHash = new Hashtable<String, Object>();
        try {
            String classpathString = System.getProperty("java.class.path");
            if (verbose)
                pwOut.println("classpath: " + classpathString);
            StringTokenizer st = new StringTokenizer(classpathString, File.pathSeparator);
            int entries = st.countTokens();
            classpath = new String[entries];
            for (int i = 0; i < entries; i++) {
                classpath[i] = st.nextToken();
            }
        } catch (SecurityException se) {
            pwOut.println("**error** SecurityException getting classpath");
            System.exit(1);
        }
        for (int i = 0; i < classpath.length; i++) {
            String pathEntry = classpath[i];
            if (pathEntry.toUpperCase(java.util.Locale.ENGLISH).endsWith(".ZIP") ||
                    pathEntry.toUpperCase(java.util.Locale.ENGLISH).endsWith(".JAR")) {
                ZipFile zipfile = null;
                try {
                    zipfile = new ZipFile(pathEntry.replace('/', File.separatorChar));
                } catch (IOException ioe) {
                    // can't do anything about it; zipfile doesn't exists
                    // it can happen if the person sticks a directory called
                    // foo.zip in the classpath or foo.zip doesn't exist as
                    // a file
                }
                if (zipfile != null) {

                    classpathHash.put(pathEntry, zipfile);
                } else {
                    if (verbose) {
                        pwOut.println("Ignoring <zip> entry: " + pathEntry);
                    }

                }
            } else {
                File file = new File(pathEntry);

                if (file.exists() && file.isDirectory()) {
                    classpathHash.put(pathEntry, file);
                } else {
                    if (verbose) {
                        pwOut.println("Ignoring <dir> entry: " + pathEntry);
                    }
                }
            }
        }
    }


    protected InputStream locateClass(String className, boolean beVerbose) {
        if (className.startsWith("/")) {
            className = className.substring(1);
        }
        if (beVerbose) {
            pwOut.println("Looking for " + className);
        }

        if (classpath == null) {
            loadClasspath();
        }

        for (int i = 0; i < classpath.length; i++) {
            String pathEntry = classpath[i];
            Object hash = classpathHash.get(pathEntry);
            if (hash != null) {
                if (hash instanceof ZipFile)
                    try {
                        ZipFile zipfile = (ZipFile) hash;

                        ZipEntry entry = zipfile.getEntry(className);

                        if (entry != null) {
                            InputStream is = zipfile.getInputStream(entry);
                            return new DataInputStream(new BufferedInputStream(is));
                        }
                    } catch (IOException ioe) {
                        if (beVerbose) {
                            pwOut.println("IOException loading ZipFile or creating InputStream " +
                                    " from it");
                            pwOut.println(ioe);
                        }
                    }
                else if (hash instanceof File) {
                    File file = new File((File) hash, className.replace('/', File.separatorChar));
                    if (beVerbose) {
                        pwOut.println("looking to load file: " + file.getName());
                    }
                    if (file.exists()) {
                        if (beVerbose) {
                            pwOut.println(" found it!");
                        }
                        try {
                            FileInputStream fis = new FileInputStream(file);
                            return new BufferedInputStream(fis, 8192);
                        } catch (IOException ioe) {
                            if (beVerbose) {
                                pwOut.println("IOException creating FileInputStream");
                                pwOut.println(ioe);
                                return null;
                            }
                        }
                    }
                }
            }
            //
        }

        // could not find it
        if (beVerbose) {
            pwOut.println("returing null on purpose");
        }
        return null;
    }

    protected void findDependencies(String className) throws IOException {
        indent++;
        try {
            if (className.startsWith("java.") && skipJava) {
                pwOut.println("Skipping JAVA " + className);
                return;
            }
            if (className.startsWith("javax.") && skipJavax) {
                //System.out.println("Skipping JAVAX " + className);
                return;
            }
            if (className.startsWith("sun.") && skipSun) {
                //System.out.println("Skipping Sun " + className);
                return;
            }
            if (className.startsWith("org.") && skipOrg) {
                // Allow opensource com.splicemachine.db classes
                if (!className.startsWith("com.splicemachine.db")) {
                    //System.out.println("Skipping org " + className);
                    return;
                }
            }
            if (className.startsWith("com.informix.") && skipInformix) {
                //System.out.println("Skipping Informix " + className);
                return;
            }
            if (className.startsWith("com.ibm.mobileservices.") && skipDB2e) {
                //System.out.println("Skipping DB2e " + className);
                return;
            }
            if (className.startsWith("common.") && skipDB2) {
                //System.out.println("Skipping DB2 common " + className);
                return;
            }

            if (ignoreWebLogic) {
                if (className.startsWith("weblogic.")) {
                    return;
                }
            }

            if (db2jtools || db2jdrda) {

                // for tools skip classes that are part of the db2j product api
                // they should be pulled in from cs.jar or any client.jar
                if (
                        className.startsWith("com.splicemachine.db.authentication.")
                                || className.startsWith("com.splicemachine.db.catalog.")
                                || className.startsWith("com.splicemachine.db.iapi.db.")
                                || className.startsWith("com.splicemachine.db.diag.")
                                || className.startsWith("com.splicemachine.db.jdbc.")
                                || className.startsWith("com.splicemachine.db.vti.")
                        ) {
                    return;
                }
            }

            // drda explicitly brings in some database engine classes.
            // they must be picke dup from cs.jar and not put in
            // the network server jar.
            if (db2jdrda) {

                if (
                        className.startsWith("com.splicemachine.db.impl.sql")
                                || className.startsWith("com.splicemachine.db.impl.jdbc")
                                || className.startsWith("com.splicemachine.db.impl.services")
                                || className.startsWith("com.splicemachine.db.iapi.")
                                || className.startsWith("com.splicemachine.db.security.")
                        ) {
                    return;
                }
            }

            // already seen class
            if (foundClasses.get(className) != null)
                return;

            if (verbose) {
                for (int i = 0; i < indent; i++) {
                    System.out.print(".");
                }
                System.out.println(className);
            }

		/*
            com.splicemachine.db.iapi.reference.ClassName &
			RegisteredFormatIds has a list of all registered classes, If we pull this in then
			we will pull in the complete set of classes. So we add this to our list but don't
			dependency check it.
		*/
            boolean dontCheckDependencies = false;
		/*
		if (className.equals("com.splicemachine.db.iapi.reference.ClassName") ||
			className.equals("com.splicemachine.db.iapi.services.io.RegisteredFormatIds")) {
			dontCheckDependencies = true;
		}
		*/


            try {
                Hashtable<String, String> localHashtable = null;

                if (keepDependencyHistory) {
                    localHashtable = masterClassList.get(className);
                    if (localHashtable == null) {
                        localHashtable = new Hashtable<String, String>();
                        masterClassList.put(className, localHashtable);
                    }
                }

                foundClasses.put(className, "class");

                if (dontCheckDependencies)
                    return;

                String fileName = "/" + className.replace('.', '/') + ".class";

                InputStream is = locateClass(fileName, false);

                if (is == null) {
                    pwOut.println("**error** Got NULL when looking for fileName = " + fileName);
                    if (!keepRolling) {
                        System.exit(1);
                    } else {
                        errorCount++;
                    }
                }
                //byte[] classData = new byte[is.available()];
                //is.read(classData);

//                ClassInvestigator ch = ClassInvestigator.load(is);
//                is.close();
//
//                for (Enumeration e = ch/*.getClassInfo()*/.referencedClasses(); e.hasMoreElements(); ) {
//                    String x = (String) e.nextElement();
//                    // skip microsoft classes
//                    if (x.startsWith("com.ms.")) {
//                        continue;
//                    }
//
////                    if (!SanityManager.DEBUG) {
////                        if (x.indexOf("SanityManager") != -1) {
////
////                            boolean printSanityWarning = true;
////
////                            int ld = className.lastIndexOf(".");
////                            if (ld != -1) {
////                                if (className.lastIndexOf("T_") == ld + 1)
////                                    printSanityWarning = false;
////                                else if (className.lastIndexOf("T_") == ld + 1)
////                                    printSanityWarning = false;
////                                else if (className.lastIndexOf("D_") == ld + 1)
////                                    printSanityWarning = false;
////                                else if (className.lastIndexOf("TEST_") == ld + 1)
////                                    printSanityWarning = false;
////                                else if (className.endsWith("SanityManager"))
////                                    printSanityWarning = false;
////                            }
////
////                            if (printSanityWarning)
////                                System.out.println("SANITY >>> " + fileName);
////                        }
////                    }
//
//                    if (keepDependencyHistory && (localHashtable.get(x) == null)) {
//
//                        localHashtable.put(x, "class");
//                    }
//                    findDependencies(x);
//                }
            } catch (NullPointerException npe) {
                pwOut.println("**error** Got NullPointerException in findDependencies when looking up ");
                pwOut.println(className);

                npe.printStackTrace();
                if (!keepRolling) {
                    System.exit(1);
                }
                errorCount++;
            }

            // look for properties only with cloudscape code ...
            if (!isCloudscapeCode(className))
                return;

            // The following block of code checks the package of each class
            // scanned to see if there is a corresponding properties file
            // from propFiles and adds it to the list of found classes.
            // derbytools.jar should not contain any of these files, so skip
            // for that jar. See also DERBY-1537.
            if (!db2jtools) {
                String packageName = className.substring(0, className.lastIndexOf('.') + 1);

                for (int i = 0; i < propFiles.length; i++) {
                    String fileName = "/" + packageName.replace('.', '/') + propFiles[i];
                    if (foundClasses.get(fileName) != null)
                        continue;

                    InputStream is = getClass().getResourceAsStream(fileName);
                    if (is == null)
                        continue;
                    is.close();


                    foundClasses.put(fileName.substring(1), "file");
                }
            }
        } finally {
            indent--;
        }
    }

    protected boolean isCloudscapeCode(String name) {
        return name.startsWith("com.ibm.db2j.") ||
                name.startsWith("com.ihost.cs.") ||
                name.startsWith("db2j.") ||
                name.startsWith("com.splicemachine.db");
    }


    protected void showAllItems() {
        Enumeration e = masterClassList.keys();
        pwOut.println("------------Printing all dependents--------------");
        while (e.hasMoreElements()) {
            String kid = (String) e.nextElement();
            pwOut.println(kid);
            Hashtable<String, Integer> scoreboard =
                    new Hashtable<String, Integer>();
            Hashtable<String, String> grandkids = masterClassList.get(kid);
            unrollHashtable("", grandkids, scoreboard, 1);
        }
    }


    protected void showAllItemsOneLevel() {
        pwOut.println("Showing all dependencies");
        pwOut.println("One level only");
        pwOut.println("-----------------------------------");

        Enumeration e = masterClassList.keys();
        while (e.hasMoreElements()) {
            String key = (String) e.nextElement();
            pwOut.println(key);
            Hashtable h = (Hashtable) masterClassList.get(key);
            Enumeration e2 = h.keys();
            Hashtable h2 = new Hashtable();
            while (e2.hasMoreElements()) {
                String key2 = (String) e2.nextElement();
                pwOut.println("\t" + key2);
            }
        }
    }


    protected void unrollHashtable(
            String parent,
            Hashtable<String, String> current,
            Hashtable<String, Integer> scoreboard,
            int indentLevel) {
        String indentString = "  ";
        Enumeration<String> e = current.keys();
        String key = null;

        while (e.hasMoreElements()) {
            key = e.nextElement();
            if (key.equals(parent)) {
                continue;
            }
            pwOut.print(indentLevel + ":");

            Integer value = scoreboard.get(key);
            if (value != null) {
                for (int i = 0; i < indentLevel; i++) {
                    pwOut.print(indentString);
                }
                pwOut.println(key + "*****REPEATED class back at level " + value + "****");
                return;
            }
            for (int i = 0; i < indentLevel; i++) {
                pwOut.print(indentString);
            }
            pwOut.println(key);

            Hashtable<String, String> currentsChildren =
                    masterClassList.get(key);
            scoreboard.put(key, indentLevel);
            unrollHashtable(key, currentsChildren, scoreboard, (indentLevel + 1));
            scoreboard.put(key, indentLevel);

        }
    }


}
