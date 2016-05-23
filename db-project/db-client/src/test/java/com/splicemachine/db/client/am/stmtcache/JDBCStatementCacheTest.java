/*

   Derby - Class com.splicemachine.dbTesting.unitTests.junit.JDBCStatementCacheTest

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

*/

package com.splicemachine.db.client.am.stmtcache;

import junit.framework.TestCase;
import org.junit.Test;

/**
 * Tests basic operation of the JDBC prepared statement object cache and the
 * keys used to operate on it.
 */
public class JDBCStatementCacheTest {

    /**
     * Make sure a negative or zero max size is not allowed, as this will in
     * effect be no caching but with an overhead.
     * <p>
     * The overhead would come from always throwing out the newly inserted
     * element.
     */
    @Test
    public void testCreateCacheWithZeroOrNegativeMaxSize() {
        try {
            new JDBCStatementCache(-10);
            TestCase.fail("Negative max size should not be allowed");
        } catch (IllegalArgumentException iae) {
            // As expected
        }
        try {
            new JDBCStatementCache(0);
            TestCase.fail("Zero max size should not be allowed");
        } catch (IllegalArgumentException iae) {
            // As expected
        }
    }


}
