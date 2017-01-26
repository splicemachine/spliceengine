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

import java.io.InputStream;

import junit.framework.Test;
import junit.framework.TestSuite;
import com.splicemachine.db.iapi.jdbc.CharacterStreamDescriptor;

/**
 * Tests basic operations on a bigger read-only Clob from the store module.
 */
public class BiggerStoreStreamClobTest
    extends InternalClobTest{

    private static final long CLOBLENGTH = 67*1024+19; // ~97 KB
    private static final long BYTES_PER_CHAR = 1; // All modern Latin

    public BiggerStoreStreamClobTest(String name) {
        super(name);
    }

    public void setUp()
            throws Exception {
        super.initialCharLength = CLOBLENGTH;
        super.headerLength = 2 +3;
        // The fake stream uses ascii. Add header and EOF marker.
        super.initialByteLength = CLOBLENGTH + headerLength;
        super.bytesPerChar = BYTES_PER_CHAR;
        EmbedStatement embStmt = (EmbedStatement)createStatement();
        InputStream is = new FakeStoreStream(CLOBLENGTH);
        CharacterStreamDescriptor csd =
                new CharacterStreamDescriptor.Builder().stream(is).
                    charLength(initialCharLength).byteLength(0L).
                    curCharPos(CharacterStreamDescriptor.BEFORE_FIRST).
                    dataOffset(2L).build();
        iClob = new StoreStreamClob(csd, embStmt);
        assertEquals(CLOBLENGTH, iClob.getCharLength());
    }

    public void tearDown()
            throws Exception {
        this.iClob.release();
        this.iClob = null;
        super.tearDown();
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(BiggerStoreStreamClobTest.class,
                                        "BiggerStoreStreamClobTest suite");
        return suite;
    }
} // End class BiggerStoreStreamClobTest
