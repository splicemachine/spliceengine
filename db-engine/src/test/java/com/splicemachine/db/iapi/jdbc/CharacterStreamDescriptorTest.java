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
package com.splicemachine.db.iapi.jdbc;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import junit.framework.Test;
import junit.framework.TestSuite;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.PositionedStream;
import com.splicemachine.dbTesting.junit.BaseTestCase;

/**
 * Simple tests of the character stream descriptor class.
 */
public class CharacterStreamDescriptorTest
    extends BaseTestCase {

    public CharacterStreamDescriptorTest(String name) {
        super(name);
    }

    /**
     * Tests the default values set by the builder.
     */
    public void testDefaultValues() {
        InputStream emptyStream = new ByteArrayInputStream(new byte[] {});
        CharacterStreamDescriptor.Builder b =
                new CharacterStreamDescriptor.Builder().stream(emptyStream);
        CharacterStreamDescriptor csd = b.build();

        // Test the default values.
        assertEquals(false, csd.isBufferable());
        assertEquals(false, csd.isPositionAware());
        assertEquals(0, csd.getDataOffset());
        assertEquals(0, csd.getCurBytePos());
        assertEquals(1, csd.getCurCharPos());
        assertEquals(0, csd.getByteLength());
        assertEquals(0, csd.getCharLength());
        assertEquals(Long.MAX_VALUE, csd.getMaxCharLength());
    }

    public void testSetValues() {
        final long charLength = 1023;
        final long byteLength = 1023*2;
        final long curBytePos = 4;
        final long curCharPos = 2;
        final long dataOffset = 2;
        final long maxCharLen = 2459;
        InputStream emptyStream = new ByteArrayInputStream(new byte[] {});

        CharacterStreamDescriptor.Builder b =
                new CharacterStreamDescriptor.Builder().bufferable(true).
                byteLength(byteLength).charLength(charLength).
                curBytePos(curBytePos).curCharPos(curCharPos).
                dataOffset(dataOffset).maxCharLength(maxCharLen).
                stream(emptyStream);
        CharacterStreamDescriptor csd = b.build();

        // Test the values.
        assertEquals(true, csd.isBufferable());
        assertEquals(false, csd.isPositionAware());
        assertEquals(dataOffset, csd.getDataOffset());
        assertEquals(curBytePos, csd.getCurBytePos());
        assertEquals(curCharPos, csd.getCurCharPos());
        assertEquals(byteLength, csd.getByteLength());
        assertEquals(charLength, csd.getCharLength());
        assertEquals(maxCharLen, csd.getMaxCharLength());

        PositionedStream emptyPS = new PositionedTestStream(curBytePos);
        // Set only a few values.
        csd = new CharacterStreamDescriptor.Builder().bufferable(true).
                positionAware(true). maxCharLength(maxCharLen).
                stream(emptyPS.asInputStream()).build();
        assertEquals(true, csd.isBufferable());
        assertEquals(true, csd.isPositionAware());
        assertEquals(maxCharLen, csd.getMaxCharLength());

        // Set data offset and update the character position accordingly.
        csd = new CharacterStreamDescriptor.Builder().bufferable(true).
                positionAware(true).dataOffset(dataOffset).
                curCharPos(CharacterStreamDescriptor.BEFORE_FIRST).
                stream(emptyPS.asInputStream()).build();
        assertEquals(true, csd.isBufferable());
        assertEquals(true, csd.isPositionAware());
        assertEquals(dataOffset, csd.getDataOffset());
        assertEquals(CharacterStreamDescriptor.BEFORE_FIRST,
                csd.getCurCharPos());

    }

    public void testCopyState() {
        final long charLength = 1023;
        final long byteLength = 1023*2;
        final long curBytePos = 4;
        final long curCharPos = 2;
        final long dataOffset = 2;
        final long maxCharLen = 3021;
        InputStream emptyStream = new ByteArrayInputStream(new byte[] {});

        CharacterStreamDescriptor.Builder b1 =
                new CharacterStreamDescriptor.Builder().bufferable(true).
                byteLength(byteLength).charLength(charLength).
                curBytePos(curBytePos).curCharPos(curCharPos).
                dataOffset(dataOffset).maxCharLength(maxCharLen).
                stream(emptyStream);
        CharacterStreamDescriptor csd1 = b1.build();
        CharacterStreamDescriptor.Builder b2 =
                new CharacterStreamDescriptor.Builder().copyState(csd1);
        CharacterStreamDescriptor csd2 = b2.build();

        // Test the values.
        assertEquals(csd2.isBufferable(), csd1.isBufferable());
        assertEquals(csd2.isPositionAware(), csd1.isPositionAware());
        assertEquals(csd2.getDataOffset(), csd1.getDataOffset());
        assertEquals(csd2.getCurBytePos(), csd1.getCurBytePos());
        assertEquals(csd2.getCurCharPos(), csd1.getCurCharPos());
        assertEquals(csd2.getByteLength(), csd1.getByteLength());
        assertEquals(csd2.getCharLength(), csd1.getCharLength());
        assertEquals(csd2.getMaxCharLength(), csd1.getMaxCharLength());
        assertTrue(csd2.getStream() == csd1.getStream());

        // Override one value.
        CharacterStreamDescriptor.Builder b3 =
                new CharacterStreamDescriptor.Builder().copyState(csd1).
                maxCharLength(8765);
        CharacterStreamDescriptor csd3 = b3.build();
        assertEquals(8765, csd3.getMaxCharLength());

        // Demonstrate that copying the state after setting a value explicitly
        // overwrites the the set value.
        CharacterStreamDescriptor.Builder b4 =
                new CharacterStreamDescriptor.Builder().
                maxCharLength(8765).
                copyState(csd1);
        CharacterStreamDescriptor csd4 = b4.build();
        assertEquals(csd1.getMaxCharLength(), csd4.getMaxCharLength());
    }

    public static Test suite() {
        return new TestSuite(CharacterStreamDescriptorTest.class,
                "CharacterStreamDescriptorTest suite");
    }

    /**
     * A test stream that implements the {@code PositionedStream} interface.
     * The stream is not functional, it always returns {@code -1}.
     */
    private static class PositionedTestStream
            extends InputStream
            implements PositionedStream {

            private final long pos;

            PositionedTestStream(long pos) {
                this.pos = pos;
            }

            public int read() throws IOException {
                return -1;
            }

            public InputStream asInputStream() {
                return this;
            }

            public long getPosition() {
                // Return the position specified in constructor.
                return pos;
            }

            public void reposition(long requestedPos)
                    throws IOException, StandardException {
                // Do nothing, this is not a functional stream.
            }
        }
    }
