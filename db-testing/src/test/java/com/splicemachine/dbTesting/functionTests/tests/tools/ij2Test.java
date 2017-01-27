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

import java.util.Locale;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.dbTesting.functionTests.util.ScriptTestCase;
import com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.LocaleTestSetup;
import com.splicemachine.dbTesting.junit.SystemPropertyTestSetup;
import com.splicemachine.dbTesting.junit.TestConfiguration;

public class ij2Test extends ScriptTestCase {

    public ij2Test(String script) {
        super(script, true);
    }

    public static Test suite() {
        Properties props = new Properties();

        // When running on JSR-169 platforms, we need to use a data source
        // instead of a JDBC URL since DriverManager isn't available.
        if (JDBC.vmSupportsJSR169()) {
            props.setProperty("ij.dataSource",
                              "com.splicemachine.db.jdbc.EmbeddedSimpleDataSource");
            props.setProperty("ij.dataSource.databaseName", "wombat");
            props.setProperty("ij.dataSource.createDatabase", "create");
        }

        props.setProperty("derby.infolog.append", "true");
        props.setProperty("ij.protocol", "jdbc:splice:");
        props.setProperty("ij.database", "wombat;create=true");

        Test test = new SystemPropertyTestSetup(new ij2Test("ij2"), props);
        test = new LocaleTestSetup(test, Locale.ENGLISH);
        test = TestConfiguration.singleUseDatabaseDecorator(test, "wombat1");
        test = new CleanDatabaseTestSetup(test);

        TestSuite suite = new TestSuite("ij2Scripts");
        suite.addTest(test);

        if (JDBC.vmSupportsJDBC3()) {
            props.setProperty("ij.protocol", "jdbc:splice:");
            props.setProperty("ij.showNoConnectionsAtStart", "true");

            Test testb = new SystemPropertyTestSetup(new ij2Test("ij2b"), props);
            testb = new LocaleTestSetup(testb, Locale.ENGLISH);
            testb = TestConfiguration.singleUseDatabaseDecorator(testb, "wombat1");
            testb = new CleanDatabaseTestSetup(testb);
            suite.addTest(testb);
        }
        return getIJConfig(suite);
    }
}
