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

package com.splicemachine.db.iapi.services.cache;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Enumeration;
import java.util.Hashtable;

/**
 * This class implements a program that catalogs the size estimate coefficients of various classes.
 *
 * @see ClassSize#getSizeCoefficients.
 * <p/>
 * The program is invoked as:
 * <p/>
 * <pre>
 *
 * java -DWS=<i>work-space</i>
 *      [ -DclassDir=class-dir ]
 *      [ -Dout=out-file
 *      [ -Dprefix[.x=package-prefix ]]
 *      [ -Dverbose=true ] com.splicemachine.db.iapi.services.cache.ClassSizeCrawler class-or-interface ...
 *
 * </pre>
 *
 * <p/>
 * This program gets the size coefficients for each class in the <i>class-or-interface</i> list,
 * and for each class that implements an interface in the list. If there is an interface in the list
 * this program crawls through the classes hierarcy, starting at points specified by the prefix
 * properties, looking for classes that implement the interfaces.
 * <p/>
 * If the <i>class-or-interface</i> list is empty then this program searches for implementations
 * of com.splicemachine.db.iapi.types.DataValueDescriptor, and at least one prefix property
 * must be specified
 * <p/>
 * The catalog is written as a java source file into <i>out-file</i>, by default
 * <i>work-space</i>/java/com.splicemachine.db.iapi.services.cache.ClassSizeCatalog.java.
 * <p/>
 * <i>work-space</i> is the directory containing the java and classes directories. $WS in the
 * standard development environment. This property is required.
 * <p/>
 * <i>class-dir</i> is the directory containing the compiled classes. By default it is <i>work-space</i>/classes.
 * <p/>
 * <i>package-prefix</i> is the first part of a package name. e.g. "com.ibm.db2j.impl". At least
 * one prefix property must be specified if there is an interface in the list.
 * <p/>
 * For example:<br>
 * <pre>
 * <code>
 * java -Dprefix.1=com.splicemachine.db.iapi.types \
 *      com.splicemachine.db.iapi.services.cache.ClassSizeCrawler \
 *        com.splicemachine.db.iapi.types.DataValueDescriptor \
 *        java.math.BigDecimal \
 *        com.splicemachine.db.impl.services.cache.Generic.CachedItem
 * </code>
 * </pre>
 */
public class ClassSizeCrawler {

    public static void main(String[] arg) {
        String[] classAndInterfaceList = {"com.splicemachine.db.iapi.types.DataValueDescriptor"};
        if (arg.length > 0) {
            classAndInterfaceList = arg;
        }
        Class[] interfaceList = new Class[classAndInterfaceList.length];
        int interfaceCount = 0;
        Class[] classList = new Class[classAndInterfaceList.length];
        int classCount = 0;

        Class classSizeClass = ClassSize.class; // Make sure that the garbage collector does not unload it
        ClassSize.setDummyCatalog();
        /* Most of the classes we will catalog invoke ClassSize.estimateBaseFromCatalog in
         * their static initializer. This dummy the catalog out so that this will not generate
         * errors. We will not actually use the classes, just examine their fields.
         */


        for (String aClassAndInterfaceList : classAndInterfaceList) {
            Class cls;
            try {
                cls = Class.forName(aClassAndInterfaceList);
            } catch (ClassNotFoundException cnfe) {
                throw new IllegalStateException("*** Could not find class " + aClassAndInterfaceList);
            }
            if (cls.isInterface())
                interfaceList[interfaceCount++] = cls;
            else
                classList[classCount++] = cls;
        }

        StringBuilder baseDir = new StringBuilder(System.getProperty("classDir", ""));
        if (baseDir.length() == 0) {
            baseDir.append("classes");
        }
        int baseDirLength = baseDir.length();

        StringBuffer packagePrefix = new StringBuffer();

        Hashtable<String, int[]> classSizes = new Hashtable<>();

        ClassSizeCrawler crawler = new ClassSizeCrawler(interfaceList, interfaceCount, classSizes);

        if (interfaceCount > 0) {
            boolean gotPrefix = false;
            // Crawl through the class hierarchies for classes implementing the interfaces
            for (Enumeration e = System.getProperties().propertyNames();
                 e.hasMoreElements(); ) {
                String propertyName = (String) e.nextElement();
                if (propertyName.equals("prefix") || propertyName.startsWith("prefix.")) {
                    gotPrefix = true;
                    packagePrefix.setLength(0);
                    packagePrefix.append(System.getProperty(propertyName));
                    baseDir.setLength(baseDirLength);
                    if (packagePrefix.length() > 0) {
                        baseDir.append('/');
                        for (int offset = 0; offset < packagePrefix.length(); offset++) {
                            char c = packagePrefix.charAt(offset);
                            if (c == '.')
                                baseDir.append('/');
                            else
                                baseDir.append(c);
                        }
                    }
                    crawler.crawl(new File(baseDir.toString()), packagePrefix);
                }
            }
            if (!gotPrefix) {
                System.err.println("*** Could not search the class hierarchy because no starting");
                System.err.println("    prefixes where specified.");
                System.exit(1);
            }
        }

        for (int i = 0; i < classCount; i++) {
            crawler.addClass(classList[i]);
        }

        baseDir.setLength(baseDirLength);
        String outputFileName = System.getProperty("out");
        try {
            File file = new File(outputFileName);
            file.getParentFile().mkdirs();
            FileWriter out1 = new FileWriter(file);
            PrintWriter out = new PrintWriter(out1);
            out.print("/*\n\n" +

                    "   Licensed to the Apache Software Foundation (ASF) under one or more\n" +
                    "   contributor license agreements.  See the NOTICE file distributed with\n" +
                    "   this work for additional information regarding copyright ownership.\n" +
                    "   The ASF licenses this file to You under the Apache License, Version 2.0\n" +
                    "   (the \"License\"); you may not use this file except in compliance with\n" +
                    "   the License.  You may obtain a copy of the License at\n" +
                    "\n" +
                    "      http://www.apache.org/licenses/LICENSE-2.0\n" +
                    "\n" +
                    "   Unless required by applicable law or agreed to in writing, software\n" +
                    "   distributed under the License is distributed on an \"AS IS\" BASIS,\n" +
                    "   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n" +
                    "   See the License for the specific language governing permissions and\n" +
                    "   limitations under the License.\n" +
                    " */\n");
            out.print("package com.splicemachine.db.iapi.services.cache;\n" +
                    "import java.util.Hashtable;\n" +
                    "class ClassSizeCatalog extends java.util.Hashtable\n" +
                    "{\n" +
                    "    ClassSizeCatalog()\n" +
                    "    {\n");
            for (Enumeration e = classSizes.keys();
                 e.hasMoreElements(); ) {
                String className = (String) e.nextElement();
                int[] coeff = (int[]) classSizes.get(className);
                out.print("        put( \"" + className + "\", new int[]{" + coeff[0] + "," + coeff[1] + "});\n");
            }
            out.print("    }\n" + "}\n");
            out.flush();
            out.close();
        } catch (IOException ioe) {
            throw new IllegalStateException("*** Cannot write to " + outputFileName, ioe);
        }
    } // end of main

    private Class<?>[] interfaceList; // Search for classes that implement these interfaces
    private int interfaceCount;
    private Hashtable<String, int[]> classSizes;
    private boolean verbose = false;

    private ClassSizeCrawler(Class[] interfaceList,
                             int interfaceCount,
                             Hashtable<String, int[]> classSizes) {
        this.interfaceList = interfaceList;
        this.classSizes = classSizes;
        this.interfaceCount = interfaceCount;
        verbose = Boolean.valueOf(System.getProperty("verbose", "false"));
    }

    private void crawl(File curDir, StringBuffer className) {
        if (verbose)
            System.out.println("Searching directory " + curDir.getPath());

        try {
            if (!curDir.isDirectory()) {
                throw new IllegalStateException("*** " + curDir.getPath() + " is not a directory.");
            }
        } catch (SecurityException se) {
            throw new IllegalStateException("Cannot access " + curDir.getPath());
        }
        String[] filenames = curDir.list();
        if (className.length() != 0)
            className.append(".");

        int classNameLength = className.length();
        for (String filename : filenames) {
            if (filename.endsWith(".class")) {
                // Strip off the ".class" suffix
                String s = filename.substring(0, filename.length() - 6);
                className.append(s);
                Class<?> targetClass = null;
                String targetClassName = className.toString();
                try {
                    targetClass = Class.forName(targetClassName);
                    if (!targetClass.isInterface()) {
                        for (int interfaceIdx = 0; interfaceIdx < interfaceCount; interfaceIdx++) {
                            if (interfaceList[interfaceIdx].isAssignableFrom(targetClass))
                                addClass(targetClass);
                        }
                    }
                } catch (ClassNotFoundException cnfe) {
                    System.err.println("Could not find class " + targetClassName);
                    System.exit(1);
                } catch (Throwable t) {
                }
                className.setLength(classNameLength);
            } else {
                File nextDir = new File(curDir, filename);
                if (nextDir.isDirectory()) {
                    className.append(filename);
                    crawl(nextDir, className);
                    className.setLength(classNameLength);
                }
            }
        }
    }

    private void addClass(Class targetClass) {
        //int[] coefficients = ClassSize.getSizeCoefficients( targetClass);
        int[] coefficients = new int[]{1, 1};
        if (verbose)
            System.out.println(targetClass.getName() + " " + coefficients[0] + ", " + coefficients[1]);
        classSizes.put(targetClass.getName(), coefficients);
    }

}
