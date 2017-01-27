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
package com.splicemachine.db.impl.jdbc;

import com.splicemachine.dbTesting.functionTests.util.streams.CharAlphabet;
import com.splicemachine.dbTesting.functionTests.util.streams.LoopingAlphabetReader;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Test basic operations on a small temporary Clob.
 * <p>
 * The test is intended to use sizes that makes the Clob stay in memory (i.e.
 * it is not being pushed to disk due to size).
 */
public class SmallTemporaryClobTest extends InternalClobTest{

    private static final long CLOBLENGTH = 1027;
    private static final long BYTES_PER_CHAR = 3;

    public SmallTemporaryClobTest(String name) {
        super(name);
    }

    /**
     * Creates a small read-write Clob that is kept in memory.
     */
    public void setUp()
            throws Exception {
        super.initialCharLength = CLOBLENGTH;
        super.headerLength = 2 + 3;
       // All tamil letters. Also add the header bytes.
        super.initialByteLength = CLOBLENGTH *3 + headerLength;
        super.bytesPerChar = BYTES_PER_CHAR;
        EmbedStatement embStmt = (EmbedStatement)createStatement();
        iClob = new TemporaryClob(embStmt);
        transferData(
            new LoopingAlphabetReader(CLOBLENGTH, CharAlphabet.tamil()),
            iClob.getWriter(1L),
            CLOBLENGTH);
        assertEquals(CLOBLENGTH, iClob.getCharLength());
    }

    public void tearDown()
            throws Exception {
        this.iClob.release();
        this.iClob = null;
        super.tearDown();
    }

    public static Test suite()
            throws Exception {
        Class<? extends TestCase> theClass = SmallTemporaryClobTest.class;
        TestSuite suite = new TestSuite(theClass, "SmallTemporaryClobTest suite");
        suite.addTest(addModifyingTests(theClass));
        return suite;
    }

} // End class SmallTemporaryClobTest
