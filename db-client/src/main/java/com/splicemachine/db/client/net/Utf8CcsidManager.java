/*
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
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.client.net;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;

import com.splicemachine.db.client.am.Agent;
import com.splicemachine.db.client.am.ClientMessageId;
import com.splicemachine.db.client.am.SqlException;
import com.splicemachine.db.shared.common.sanity.SanityManager;
import com.splicemachine.db.shared.common.reference.SQLState;

public class Utf8CcsidManager extends CcsidManager {

    private final static String UTF8 = "UTF-8";
    private final static Charset UTF8_CHARSET = Charset.forName(UTF8);
    private final CharsetEncoder encoder = UTF8_CHARSET.newEncoder();

    public Utf8CcsidManager() {
        super((byte) ' ', // 0x40 is the ebcdic space character
                (byte) '.',
                new byte[]{//02132002jev begin
                    //     '0',       '1',       '2',       '3',      '4',
                    (byte) 0xf0, (byte) 0xf1, (byte) 0xf2, (byte) 0xf3, (byte) 0xf4,
                    //     '5',       '6',       '7',       '8',      '9',
                    (byte) 0xf5, (byte) 0xf6, (byte) 0xf7, (byte) 0xf8, (byte) 0xf9,
                    //     'A',       'B',       'C',       'D',      'E',
                    (byte) 0xc1, (byte) 0xc2, (byte) 0xc3, (byte) 0xc4, (byte) 0xc5,
                    //      'F'
                    (byte) 0xc6},
                new byte[]{
                    //     'G',       'H',       'I',       'J',      'K',
                    (byte) 0xc7, (byte) 0xc8, (byte) 0xc9, (byte) 0xd1, (byte) 0xd2,
                    //     'L',       'M',       'N',       '0',      'P',
                    (byte) 0xd3, (byte) 0xd4, (byte) 0xd5, (byte) 0xd6, (byte) 0xd7,
                    //     'A',       'B',       'C',       'D',      'E',
                    (byte) 0xc1, (byte) 0xc2, (byte) 0xc3, (byte) 0xc4, (byte) 0xc5,
                    //      'F'
                    (byte) 0xc6}                     //02132002jev end
        );
    }
    
    public byte[] convertFromJavaString(String sourceString, Agent agent)
            throws SqlException {
        try {
            ByteBuffer buf = encoder.encode(CharBuffer.wrap(sourceString));

            if (buf.limit() == buf.capacity()) {
                // The length of the encoded representation of the string
                // matches the length of the returned buffer, so just return
                // the backing array.
                return buf.array();
            }

            // Otherwise, copy the interesting bytes into an array with the
            // correct length.
            byte[] bytes = new byte[buf.limit()];
            buf.get(bytes);
            return bytes;
        } catch (CharacterCodingException cce) {
            throw new SqlException(agent.logWriter_,
                    new ClientMessageId(SQLState.CANT_CONVERT_UNICODE_TO_UTF8),
                    cce);
        }
    }

    /**
     * Offset and numToConvert are given in terms of bytes! Not characters!
     */
    public String convertToJavaString(byte[] sourceBytes, int offset, int numToConvert) {
        try {
            // Here we'd rather specify the encoding using a Charset object to
            // avoid the need to handle UnsupportedEncodingException, but that
            // constructor wasn't introduced until Java 6.
            return new String(sourceBytes, offset, numToConvert, UTF8);
        } catch (UnsupportedEncodingException e) {
            // We don't have an agent in this method
            if (SanityManager.DEBUG) {
                SanityManager.THROWASSERT("Could not convert byte[] to Java String using UTF-8 encoding with offset",e);
            }
        }
        return null;
    }

    public void startEncoding() {
        encoder.reset();
    }

    public boolean encode(CharBuffer src, ByteBuffer dest, Agent agent)
            throws SqlException {
        CoderResult result = encoder.encode(src, dest, true);
        if (result == CoderResult.UNDERFLOW) {
            // We've exhausted the input buffer, which means we're done if
            // we just get everything flushed to the destination buffer.
            result = encoder.flush(dest);
        }

        if (result == CoderResult.UNDERFLOW) {
            // Input buffer is exhausted and everything is flushed to the
            // destination. We're done.
            return true;
        } else if (result == CoderResult.OVERFLOW) {
            // Need more room in the output buffer.
            return false;
        } else {
            // Something in the input buffer couldn't be encoded.
            throw new SqlException(agent.logWriter_,
                    new ClientMessageId(SQLState.CANT_CONVERT_UNICODE_TO_UTF8));
        }
    }
}
