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

package com.splicemachine.dbTesting.functionTests.tests.tools;

import java.util.Locale;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.dbTesting.functionTests.util.ScriptTestCase;
import com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup;
import com.splicemachine.dbTesting.junit.LocaleTestSetup;
import com.splicemachine.dbTesting.junit.SupportFilesSetup;
import com.splicemachine.dbTesting.junit.TestConfiguration;

/**
 *	Test that runs the importExportThruIJ.sql script and compares the output 
 *	to importExportThruIJ.out.
 */
public final class ImportExportIJTest extends ScriptTestCase {
	
	/**
	 * Constructor that runs a single script.
	 * 
	 * @param script - the name of the script
	 */
	private ImportExportIJTest(String script) {
		super(script, true);
	}

	
	/**
	 * Return the suite that runs the script.
	 */
	public static Test suite() {
		TestSuite suite = new TestSuite("importExportIJ");
		
        // only run with embedded
        // network server makes slightly different output
        // ('statement executed' instead of '# rows inserted/deteled', etc.)
        // and this test would never work if the server were on 
        // a remote system because the export file would be on the
        // server side, and import would be looking on the client.
        // Also, running client & embedded would require some cleanup magic to
        // remove the exported files (see e.g. ImportExportTest).
		Test test = new ImportExportIJTest("importExportIJ");
		
        // This test should run in English locale since it compares error
        // messages against a canon based on the English message text. Also,
        // run the test in a fresh database, since the language of the message
        // text is determined when the database is created.        
        test = new LocaleTestSetup(test, Locale.ENGLISH);	
        test = TestConfiguration.singleUseDatabaseDecorator(test);
		
		suite.addTest(new CleanDatabaseTestSetup(test));

        return new SupportFilesSetup(suite, new String[] {
            "functionTests/testData/ImportExport/TwoLineBadEOF.dat",
            "functionTests/testData/ImportExport/NoEOR.dat",
            "functionTests/testData/ImportExport/Access1.txt",
            "functionTests/testData/ImportExport/AccountData_defaultformat.dat",
            "functionTests/testData/ImportExport/AccountData_format1.dat",
            "functionTests/testData/ImportExport/AccountData_format2.dat",
            "functionTests/testData/ImportExport/AccountData_format2oops.dat",
            "functionTests/testData/ImportExport/AccountData_NullFields.dat",
            "functionTests/testData/ImportExport/UnsupportedFormat1.dat",
            "functionTests/testData/ImportExport/UnsupportedFormat2.dat",
            "functionTests/testData/ImportExport/db-2193.txt",
            "functionTests/testData/ImportExport/db-2193-linenumber.txt"
            }
        );
	}
}
