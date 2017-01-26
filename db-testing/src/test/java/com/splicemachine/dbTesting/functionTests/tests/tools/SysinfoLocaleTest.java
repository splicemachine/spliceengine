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

package com.splicemachine.dbTesting.functionTests.tests.tools;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Locale;
import java.util.Properties;
import junit.framework.Test;
import junit.framework.TestSuite;
import com.splicemachine.dbTesting.junit.BaseTestCase;
import com.splicemachine.dbTesting.junit.Derby;
import com.splicemachine.dbTesting.junit.SecurityManagerSetup;
import com.splicemachine.dbTesting.junit.SystemPropertyTestSetup;

/**
 * This test verifies that <code>sysinfo</code> correctly localizes its
 * messages according to the default locale and <code>db.ui.locale</code>.
 */
public class SysinfoLocaleTest extends BaseTestCase {

    /** The encoding sysinfo should use for its output. */
    private static final String ENCODING = "UTF-8";

    /** Default locale to run this test under. */
    private final Locale defaultLocale;

    /**
     * Tells whether or not this test expects sysinfo's output to be localized
     * to German.
     */
    private final boolean localizedToGerman;

    /** Name of the test. */
    private final String name;

    /** The default locale before this test started. */
    private Locale savedLocale;

    /**
     * Create a test.
     *
     * @param defaultLocale the default locale for this test
     * @param german true if output is expected to be localized to German
     * @param info extra information to append to the test name (for debugging)
     */
    private SysinfoLocaleTest(Locale defaultLocale, boolean german,
                              String info) {
        super("testSysinfoLocale");
        this.defaultLocale = defaultLocale;
        this.localizedToGerman = german;
        this.name = super.getName() + ":" + info;
    }

    /**
     * Returns the name of the test, which includes the default locale and
     * db.ui.locale to aid debugging.
     *
     * @return name of the test
     */
    public String getName() {
        return name;
    }

    /**
     * Set up the test environment.
     */
    protected void setUp() {
        savedLocale = Locale.getDefault();
        Locale.setDefault(defaultLocale);
    }

    /**
     * Tear down the test environment.
     */
    protected void tearDown() throws Exception {
        Locale.setDefault(savedLocale);
        savedLocale = null;
        super.tearDown();
    }

    /**
     * Create a suite of tests.
     *
     * @return a test suite with different combinations of
     * <code>db.ui.locale</code> and default locale
     */
    public static Test suite() {
        if (!Derby.hasTools()) {
            return new TestSuite("empty: no tools support");
        }

        TestSuite suite = new TestSuite("SysinfoLocaleTest");

        // Create test combinations. Messages should be localized to the
        // locale specified by db.ui.locale, if it's set. Otherwise, the
        // JVM's default locale should be used.
        suite.addTest(createTest(Locale.ITALY, null, false));
        suite.addTest(createTest(Locale.ITALY, "it_IT", false));
        suite.addTest(createTest(Locale.ITALY, "de_DE", true));
        suite.addTest(createTest(Locale.GERMANY, null, true));
        suite.addTest(createTest(Locale.GERMANY, "it_IT", false));
        suite.addTest(createTest(Locale.GERMANY, "de_DE", true));

        // This test creates a class loader. We don't want to grant that
        // permission to derbyTesting.jar since that means none of the tests
        // will notice if one of the product jars misses a privileged block
        // around the creation of a class loader.
        return SecurityManagerSetup.noSecurityManager(suite);
    }

    /**
     * Create a single test case.
     *
     * @param loc default locale for the test case
     * @param ui <code>db.ui.locale</code> for the test case
     * @param german whether output is expected to be German
     */
    private static Test createTest(Locale loc, String ui, boolean german) {
        Properties prop = new Properties();
        if (ui != null) {
            prop.setProperty("derby.ui.locale", ui);
        }
        // always set the encoding so that we can reliably read the output
        prop.setProperty("derby.ui.codeset", ENCODING);

        String info = "defaultLocale=" + loc + ",uiLocale=" + ui;
        Test test = new SysinfoLocaleTest(loc, german, info);
        return new SystemPropertyTestSetup(test, prop);
    }

    /**
     * Run a sysinfo class that is loaded in a separate class loader. A
     * separate class loader is required in order to force sysinfo to re-read
     * <code>db.ui.locale</code> (happens when the class is loaded).
     */
    private static void runSysinfo() throws Exception {
        final String className = "com.splicemachine.db.tools.sysinfo";
        URL sysinfoURL = SecurityManagerSetup.getURL(className);
        URL emmaURL = SecurityManagerSetup.getURL("com.vladium.emma.EMMAException");
        URL[] urls = null;
        if(emmaURL != null) {
            urls = new URL[] { sysinfoURL, emmaURL };
        } else {
            urls = new URL[] { sysinfoURL };
        }
        URLClassLoader loader = new URLClassLoader(urls, null);

        Class copy = Class.forName(className, true, loader);
        Method main = copy.getMethod("main", new Class[] { String[].class });
        main.invoke(null, new Object[] { new String[0] });
    }

    /**
     * Run sysinfo and return its output as a string.
     *
     * @return output from sysinfo
     */
    private static String getSysinfoOutput() throws Exception {
        final PrintStream savedSystemOut = System.out;
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        try {
            System.setOut(new PrintStream(bytes, true, ENCODING));
            runSysinfo();
        } finally {
            System.setOut(savedSystemOut);
        }

        return bytes.toString(ENCODING);
    }

    /**
     * Some German strings that are expected to be in sysinfo's output when
     * localized to German.
     */
    private static final String[] GERMAN_STRINGS = {
        "Name des Betriebssystems",
        "Java-Benutzerausgangsverzeichnis",
        "Derby-Informationen",
        "Informationen zur L\u00E4ndereinstellung",
    };

    /**
     * Some Italian strings that are expected to be in sysinfo's output when
     * localized to Italian.
     */
    private static final String[] ITALIAN_STRINGS = {
        "Nome SO",
        "Home utente Java",
        "Informazioni su Derby",
        "Informazioni sulla locale",
    };

    /**
     * Checks that all the expected substrings are part of the output from
     * sysinfo. Fails if one or more of the substrings are not found.
     *
     * @param expectedSubstrings substrings in the expected locale
     * @param output the output from sysinfo
     */
    private void assertContains(String[] expectedSubstrings, String output) {
        for (int i = 0; i < expectedSubstrings.length; i++) {
            String s = expectedSubstrings[i];
            if (output.indexOf(s) == -1) {
                fail("Substring '" + s + "' not found in output: " + output);
            }
        }
    }

    /**
     * Test method which checks that the output from sysinfo is correctly
     * localized.
     */
    public void testSysinfoLocale() throws Exception {
        String output = getSysinfoOutput();
        String[] expectedSubstrings =
                localizedToGerman ? GERMAN_STRINGS : ITALIAN_STRINGS;
        assertContains(expectedSubstrings, output);
    }
}
