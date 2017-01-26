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
package com.splicemachine.db.iapi.util;

import com.splicemachine.db.iapi.types.CharStreamHeaderGenerator;
import com.splicemachine.db.iapi.types.ClobStreamHeaderGenerator;
import com.splicemachine.db.iapi.types.ReaderToUTF8Stream;
import com.splicemachine.dbTesting.functionTests.util.streams.CharAlphabet;
import com.splicemachine.dbTesting.functionTests.util.streams.LoopingAlphabetReader;
import com.splicemachine.dbTesting.functionTests.util.streams.LoopingAlphabetStream;
import com.splicemachine.dbTesting.junit.BaseTestCase;
import junit.framework.TestCase;
import org.junit.Test;

import java.io.*;

/**
 * Tests that <code>skipFully</code> and <code>skipUntilEOF</code> behaves
 * correctly on Derby's modfied UTF-8 encoded streams.
 * <p>
 * These tests are dependent on the behavior of <code>ReaderToUTF8Stream</code>.
 * Note that this class inserts two bytes at the start of the user/application
 * stream to encode the length of the stream. These two bytes may be zero, even
 * if the stream is short enough for its length to be encoded.
 * <p>
 * Also note that the lengths chosen for large streams are just suitably large
 * integers. The point is to choose them large enough to make sure buffer
 * boundaries are crossed.
 * 
 * @see ReaderToUTF8Stream
 * @see UTF8Util
 */
public class UTF8UtilTest {

    /** Type name passed to {@code ReaderToUTF8Stream}. */
    private static final String TYPENAME = "VARCHAR";

    /**
     * Hardcoded header length. This is why the Clob stream header generator
     * is invoked with {@code true} in the constructor.
     */
    private static final int HEADER_LENGTH = 2;


    /**
     * Ensure the assumption that the default looping alphabet stream and the
     * modified UTF-8 encoding is equal.
     * <p>
     * If this assumption is broken, several of the other tests will fail.
     */
    @Test
    public void testEqualityOfModifedUTF8AndASCII()
            throws IOException {
        final int length = 12706;
        InputStream ascii = new LoopingAlphabetStream(length);
        InputStream modUTF8 = new ReaderToUTF8Stream(
                                    new LoopingAlphabetReader(length),
                                    length, 0, TYPENAME,
                                    new CharStreamHeaderGenerator());
        modUTF8.skip(HEADER_LENGTH); // Skip encoded length added by ReaderToUTF8Stream.
        BaseTestCase.assertEquals(ascii, modUTF8);
    }

    @Test
    public void testSkipUntilEOFOnZeroLengthStream()
            throws IOException {
        TestCase.assertEquals(0, UTF8Util.skipUntilEOF(new LoopingAlphabetStream(0)));
    }

    @Test
    public void testSkipUntilEOFOnShortStreamASCII()
            throws IOException {
        TestCase.assertEquals(5, UTF8Util.skipUntilEOF(new LoopingAlphabetStream(5)));
    }

    @Test
    public void testSkipUntilEOFOnShortStreamCJK()
            throws IOException {
        final int charLength = 5;
        InputStream in = new ReaderToUTF8Stream(
                new LoopingAlphabetReader(charLength, CharAlphabet.cjkSubset()),
                charLength, 0, TYPENAME, new CharStreamHeaderGenerator());
        in.skip(HEADER_LENGTH); // Skip encoded length added by ReaderToUTF8Stream.
        TestCase.assertEquals(charLength, UTF8Util.skipUntilEOF(in));
    }

    @Test
    public void testSkipUntilEOFOnLongStreamASCII()
            throws IOException {
        TestCase.assertEquals(127019, UTF8Util.skipUntilEOF(
                new LoopingAlphabetStream(127019)));
    }

    @Test
    public void testSkipUntilEOFOnLongStreamCJK()
            throws IOException {
        final int charLength = 127019;
        InputStream in = new ReaderToUTF8Stream(
                new LoopingAlphabetReader(charLength, CharAlphabet.cjkSubset()),
                charLength, 0, TYPENAME, new ClobStreamHeaderGenerator(true));
        in.skip(HEADER_LENGTH); // Skip encoded length added by ReaderToUTF8Stream.
        TestCase.assertEquals(charLength, UTF8Util.skipUntilEOF(in));
    }

    /**
     * Tests that <code>skipFully</code> successfully skips the requested
     * characters and returns the correct number of bytes skipped.
     * 
     * @throws IOException if the test fails for some unexpected reason
     */
    @Test
    public void testSkipFullyOnValidLongStreamCJK()
            throws IOException {
        final int charLength = 161019;
        InputStream in = new ReaderToUTF8Stream(
                new LoopingAlphabetReader(charLength, CharAlphabet.cjkSubset()),
                charLength, 0, TYPENAME, new CharStreamHeaderGenerator());
        in.skip(HEADER_LENGTH); // Skip encoded length added by ReaderToUTF8Stream.
        // Returns count in bytes, we are using CJK chars so multiply length
        // with 3 to get expected number of bytes.
        TestCase.assertEquals(charLength *3, UTF8Util.skipFully(in, charLength));
    }

    /**
     * Tests that <code>skipFully</code> throws exception if the stream contains
     * less characters than the requested number of characters to skip.
     * 
     * @throws IOException if the test fails for some unexpected reason
     */
    @Test
    public void testSkipFullyOnTooShortStreamCJK()
            throws IOException {
        final int charLength = 161019;
        InputStream in = new ReaderToUTF8Stream(
                new LoopingAlphabetReader(charLength, CharAlphabet.cjkSubset()),
                charLength, 0, TYPENAME, new ClobStreamHeaderGenerator(true));
        in.skip(HEADER_LENGTH); // Skip encoded length added by ReaderToUTF8Stream.
        try {
            UTF8Util.skipFully(in, charLength + 100);
            TestCase.fail("Should have failed because of too short stream.");
        } catch (EOFException eofe) {
            // As expected, do nothing.
        }
    }
    
    /**
     * Tests that <code>skipFully</code> throws exception if there is a UTF-8
     * encoding error in the stream
     * 
     * @throws IOException if the test fails for some unexpected reason
     */
    @Test
    public void testSkipFullyOnInvalidStreamCJK()
            throws IOException {
        final int charLength = 10;
        InputStream in = new ReaderToUTF8Stream(
                new LoopingAlphabetReader(charLength, CharAlphabet.cjkSubset()),
                charLength, 0, TYPENAME, new CharStreamHeaderGenerator());
        in.skip(HEADER_LENGTH); // Skip encoded length added by ReaderToUTF8Stream.
        in.skip(1L); // Skip one more byte to trigger a UTF error.
        try {
            UTF8Util.skipFully(in, charLength);
            TestCase.fail("Should have failed because of UTF error.");
        } catch (UTFDataFormatException udfe) {
            // As expected, do nothing.
        }
    }

    /**
     * Tests a sequence of skip calls.
     */
    @Test
    public void testMixedSkipOnStreamTamil()
            throws IOException {
        final int charLength = 161019;
        InputStream in = new ReaderToUTF8Stream(
                new LoopingAlphabetReader(charLength, CharAlphabet.tamil()),
                charLength, 0, TYPENAME, new CharStreamHeaderGenerator());
        // Skip encoded length added by ReaderToUTF8Stream.
        in.skip(HEADER_LENGTH);
        int firstSkip = 10078;
        TestCase.assertEquals(firstSkip*3, UTF8Util.skipFully(in, firstSkip));
        TestCase.assertEquals(charLength - firstSkip, UTF8Util.skipUntilEOF(in));
        try {
            UTF8Util.skipFully(in, 1L);
            TestCase.fail("Should have failed because the stream has been drained.");
        } catch (EOFException eofe) {
            // As expected, do nothing
        }
    }

    /**
     * Tries to skip characters where the data is incomplete.
     * <p>
     * In this test, the encoding states there is a character represented by
     * two bytes present. However, only one byte is provided.
     */
    @Test
    public void testMissingSecondByteOfTwo()
            throws IOException {
        // 0xdf = 11011111
        byte[] data = {'a', (byte)0xdf};
        InputStream is = new ByteArrayInputStream(data);
        try {
            UTF8Util.skipFully(is, 2);
            TestCase.fail("Reading invalid UTF-8 should fail");
        } catch (UTFDataFormatException udfe) {
            // As expected
        }
    }

    /**
     * Tries to skip characters where the data is incomplete.
     * <p>
     * In this test, the encoding states there is a character represented by
     * three bytes present. However, only one byte is provided.
     */
    @Test
    public void testMissingSecondByteOfThree()
            throws IOException {
        // 0xef = 11101111
        byte[] data = {'a', (byte)0xef};
        InputStream is = new ByteArrayInputStream(data);
        try {
            UTF8Util.skipFully(is, 2);
            TestCase.fail("Reading invalid UTF-8 should fail");
        } catch (UTFDataFormatException udfe) {
            // As expected
        }
    }

    /**
     * Tries to skip characters where the data is incomplete.
     * <p>
     * In this test, the encoding states there is a character represented by
     * three bytes present. However, only two bytes are provided.
     */
    @Test
    public void testMissingThirdByteOfThree()
            throws IOException {
        // 0xef = 11101111, 0xb8 = 10111000
        byte[] data = {'a', (byte)0xef, (byte)0xb8};
        InputStream is = new ByteArrayInputStream(data);
        try {
            UTF8Util.skipFully(is, 2);
            TestCase.fail("Reading invalid UTF-8 should fail");
        } catch (UTFDataFormatException udfe) {
            // As expected
        }
    }

    /**
     * Tries to read a stream of data where there is an invalid UTF-8 encoded
     * byte.
     */
    @Test
    public void testInvalidUTF8Encoding()
            throws IOException {
        // 0xf8 = 11111000 <-- invalid UTF-8 encoding
        byte[] data = {'a', 'b', 'c', (byte)0xf8, 'e', 'f'};
        InputStream is = new ByteArrayInputStream(data);
        try {
            UTF8Util.skipFully(is, 6);
            TestCase.fail("Reading invalid UTF-8 should fail");
        } catch (UTFDataFormatException udfe) {
            // As expected when reading invalid data
        }
    }

    /**
     * Demonstrates that skipping incorrectly encoded character sequences
     * works because the stream is not checked for well-formedness.
     */
    @Test
    public void testSkippingInvalidEncodingWorks()
            throws IOException {
        // The array contains three valid characters and one invalid three-byte
        // representation that only has two bytes present.
        // When skipping, this sequence is (incorrectly) taken as a sequence of
        // three characters ('a' - some three byte character - 'a').
        // 0xef = 11101111, 0xb8 = 10111000
        byte[] data = {'a', (byte)0xef, (byte)0xb8, 'a', 'a'};
        byte[] dataWithLength =
            {0x0, 0x5, 'a', (byte)0xef, (byte)0xb8, 'a', 'a'};
        InputStream is = new ByteArrayInputStream(data);
        // This is actually incorrect, but does work currently.
        UTF8Util.skipFully(is, 3);
        // Verify that decoding this actually fails.
        DataInputStream dis = new DataInputStream(
                                    new ByteArrayInputStream(dataWithLength));
        try {
            dis.readUTF();
            TestCase.fail("UTF-8 expected to be invalid, read should fail");
        } catch (UTFDataFormatException udfe) {
            // This is expected, since the UTF-8 encoding is invalid
        }
    }

}
