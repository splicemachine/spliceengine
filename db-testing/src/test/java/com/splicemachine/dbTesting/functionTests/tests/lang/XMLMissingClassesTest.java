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
package com.splicemachine.dbTesting.functionTests.tests.lang;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.dbTesting.junit.XML;
import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.TestConfiguration;

import java.sql.Statement;

/**
 * This JUnit test is only run when the test classpath does not
 * contain the external classes required for use of the Derby
 * XML operators--namely, JAXP and Xalan.  In such a situation
 * all we do is try to execute each of the four SQL/XML operators
 * supported by Derby.  In all cases the operators should fail
 * with an error indicating that the required classes could
 * not be found.
 */
public final class XMLMissingClassesTest extends BaseJDBCTestCase {
    
    /**
     * Statements to run if the required XML classes are missing--need
     * to make sure the result is a compile-time error in all cases.
     */
    private static String [] SQLXML_STMTS = new String []
    {
        // One statement for each of the SQL/XML operators...
        "insert into xt values " +
            "(1, xmlparse(document '<hi/>' preserve whitespace))",
        "select xmlserialize(x as char(80)) from xt",
        "select xmlexists('//*' passing by ref x) from xt",
        "select i from xt where " +
            "xmlquery('//*' passing by ref x empty on empty) is not null"
    };

    /**
     * Public constructor required for running test as standalone JUnit.
     */
    public XMLMissingClassesTest(String name)
    {
        super(name);
    }

    /**
     * If the classpath does not have the XML classes that are
     * required for using Derby's SQL/XML operators, then try
     * try to execute each of the Derby XML operators and
     * verify that the result is an error in all cases.
     *
     * If the classpath *does* have the XML classes required
     * for use of Derby SQL/XML operators, then just return
     * an empty suite (the operators are tested in a different
     * JUnit test--namely XMLTypeAndOpTests.java).
     */
    public static Test suite()
    {
        TestSuite suite = new TestSuite("XML Missing Classes Suite");
        if (!XML.classpathMeetsXMLReqs())
        {
            // Run this test in embedded and client modes.
            suite.addTest(TestConfiguration.defaultSuite(
                XMLMissingClassesTest.class));
        }

        return suite;
    }

    /**
     * Assumption is that we only run this test if the classpath
     * is missing required XML classes.  In that case do a simple
     * check to make sure any attempts to use any of the SQL/XML
     * operators will fail at compile time with the appropriate
     * error.
     */
    public void testMissingClasses() throws Exception
    {
        Statement st = createStatement();

        // It's okay to create a column of type XML, so long
        // as no operators are involved.

        st.execute("create table xt (i int, x xml)");
        st.execute("create table xt1 (i int, x xml default null)");

        // But if the create statement uses an operator, it should
        // fail.

        assertCompileError("XML00",
            "create table fail1 (i int, x xml check "
            + "(xmlexists('//should' passing by ref x)))");

        assertCompileError("XML00",
            "create table fail2 (i int, x xml default xmlparse("
            + "document '<my>default col</my>' preserve whitespace))");

        // As a sanity check, make sure that XML columns declared
        // with invalid values still throw the correct errors--
        // and especially, make sure no attempts are made to load
        // XML classes.

        assertCompileError("42894",
            "create table fail3 (i int, x xml default 'oops')");

        assertCompileError("42894",
            "create table fail4 (i int, x xml default 8)");

        assertCompileError("42818",
            "create table fail5 (i int, x xml check (x != 0))");

        // Now go through and test each of the operators.  They
        // should all fail at compile time.

        for (int i = 0; i < SQLXML_STMTS.length; i++)
            assertCompileError("XML00", SQLXML_STMTS[i]);

        // Cleanup.

        st.execute("drop table xt");
        st.execute("drop table xt1");
        st.close();
        st = null;
    }
}
