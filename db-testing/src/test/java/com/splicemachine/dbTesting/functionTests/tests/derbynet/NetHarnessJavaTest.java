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
package com.splicemachine.dbTesting.functionTests.tests.derbynet;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.dbTesting.functionTests.util.HarnessJavaTest;
import com.splicemachine.dbTesting.junit.Derby;
import com.splicemachine.dbTesting.junit.TestConfiguration;
import com.splicemachine.dbTesting.junit.SupportFilesSetup;

/**
 * NetHarnessJavaTest includes .java tests in the derbynet directory that
 * have not been converted to junit and do not have multiple masters.
 * 
 * The following tests could not be run this way, reasons for the
 * 
 * dblook_test_net - filters output
 * dblook_test_net_territory - filters output 
 * getCurrentProperties - ExceptionInInitializerError, needs investigation
 * maxthreads - forks VM
 * runtimeinfo" - filters output
 * sysinfo" - forks VM
 * sysinfo_withproperties" - forks VM
 * testij" - filters output
 * timeslice" - forks VM
 * DerbyNetAutoStart" - forks VM
 */
public class NetHarnessJavaTest extends HarnessJavaTest {
    
    /**
     * Only allow construction from our suite method.
     * 
     * @param name the name of the test to execute
     */
    private NetHarnessJavaTest(String name) {
        super(name);
     }

    /**
     * Run tests from the functionTests/tests/derbynet directory.
     */
    protected String getArea() {
        return "derbynet";
    }
    
    public static Test suite()
    {
        TestSuite suite = new TestSuite("derbynet: old harness java tests");
        
        if (!Derby.hasServer())
            return suite;

        suite.addTest(TestConfiguration.clientServerDecorator(
        		         decorate(new NetHarnessJavaTest("executeUpdate"))));

        //DERBY-2348: SECMEC 9 is available on IBM 1.4.2 and 1.5 VMs, leading
        //            to differences in output, disabling for now. While tests
        //            for security mechanism exist in NSSecurityMechanismTest,
        //            that test does not currently check the correct order of
        //            responses of secmec and secchkcd for various error cases,
        //            which is tested in ProtocolTest.
        return new SupportFilesSetup(suite,
        	           new String[] {
	                       "functionTests/tests/derbynet/excsat_accsecrd1.inc",
	                       "functionTests/tests/derbynet/excsat_accsecrd2.inc",
	                       "functionTests/tests/derbynet/excsat_secchk.inc",
	                       "functionTests/tests/derbynet/connect.inc",
	                       "functionTests/tests/derbynet/values1.inc",
	                       "functionTests/tests/derbynet/values64kblksz.inc"
	                   });
    }

}
