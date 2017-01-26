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
package com.splicemachine.db.impl.drda;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

/**
 * Adapter written to allow for protocol testing from the test package.
 * <p>
 * The only purpose of this class is to make certain constants and methods
 * that are package private available outside of this package for testing
 * purposes. See DERBY-2031.
 */
public class ProtocolTestAdapter {

    public static final byte SPACE = new EbcdicCcsidManager().space;
    /* Various constants we need to export. */
    public static final int CP_SQLCARD = CodePoint.SQLCARD;
    public static final int CP_SVRCOD = CodePoint.SVRCOD;
    public static final int CP_CODPNT = CodePoint.CODPNT;
    public static final int CP_PRCCNVCD = CodePoint.PRCCNVCD;
    public static final int CP_SYNERRCD = CodePoint.SYNERRCD;
    public static final int CP_MGRLVLLS = CodePoint.MGRLVLLS;
    public static final int CP_PRCCNVRM = CodePoint.PRCCNVRM;
    public static final int CP_SYNTAXRM = CodePoint.SYNTAXRM;
    public static final int CP_MGRLVLRM = CodePoint.MGRLVLRM;
    public static final int CP_SECMEC = CodePoint.SECMEC;
    public static final int CP_SECCHKCD = CodePoint.SECCHKCD;

    private final CcsidManager ccsidManager = new EbcdicCcsidManager();
    private final DDMWriter writer = new DDMWriter(null, null);
    private final Socket socket;
    private final DDMReader reader;
    private final OutputStream out;

    /** Initializes the adapter for use with the given socket. */
    public ProtocolTestAdapter(Socket socket)
            throws IOException {
        this.socket = socket;
        this.reader = new DDMReader(socket.getInputStream());
        this.out = socket.getOutputStream();
    }

    /** Closes the resources associated with the adapter. */
    public void close()
            throws IOException {
        // According to the JavaDoc this will also close the associated streams.
        socket.close();
    }

    /**
     * Returns the name of the given code point.
     *
     * @param codePoint code point to look up
     * @return Code point name, or {@code null} if code point is unknown.
     */
    public String lookupCodePoint(int codePoint) {
        return CodePointNameTable.lookup(codePoint);
    }

    /**
     * Returns the code point id for the given code point name.
     *
     * @param codePointName the name of the code point to look up
     * @return The code point identifier, or {@code null} if the code point
     *      name is unknown.
     */
    public Integer decodeCodePoint(String codePointName) {
        return CodePointNameTable.lookup(codePointName);
    }

    /** Converts a string to a byte array according to the CCSID manager. */
    public byte[] convertFromJavaString(String str) {
        return ccsidManager.convertFromJavaString(str);
    }

    /** Instructs the {@code DDMReader} and {@code DDMWriter} to use UTF-8. */
    public void setUtf8Ccsid() {
        writer.setUtf8Ccsid();
        reader.setUtf8Ccsid();
    }

    /* DDMWriter forwarding methods */

    public void wCreateDssRequest() {
        writer.createDssRequest();
    }

    public void wCreateDssObject() {
        writer.createDssObject();
    }

    public void wCreateDssReply() {
        writer.createDssReply();
    }

    public void wEndDss() {
        writer.endDss();
    }

    public void wEndDss(byte b) {
        writer.endDss(b);
    }

    public void wEndDdm() {
        writer.endDdm();
    }

    public void wEndDdmAndDss() {
        writer.endDdmAndDss();
    }

    public void wStartDdm(int cp) {
        writer.startDdm(cp);
    }

    public void wWriteScalarString(int cp, String str) {
        writer.writeScalarString(cp, str);
    }

    public void wWriteScalar2Bytes(int cp, int value) {
        writer.writeScalar2Bytes(cp, value);
    }

    public void wWriteScalar1Byte(int cp, int value) {
        writer.writeScalar1Byte(cp, value);
    }

    public void wWriteScalarBytes(int cp, byte[] buf) {
        writer.writeScalarBytes(cp, buf);
    }
    public void wWriteScalarPaddedBytes(int cp, byte[] buf,
                                        int length, byte ch) {
        writer.writeScalarPaddedBytes(cp, buf, length, ch);
    }

    public void wWriteByte(int b) {
        writer.writeByte(b);
    }

    public void wWriteBytes(byte[] buf) {
        writer.writeBytes(buf);
    }

    public void wWriteShort(int v) {
        writer.writeShort(v);
    }

    public void wWriteInt(int v) {
        writer.writeInt(v);
    }

    public void wWriteCodePoint4Bytes(int cp, int v) {
        writer.writeCodePoint4Bytes(cp, v);
    }

    public void wPadBytes(byte ch, int len) {
        writer.padBytes(ch, len);
    }

    public void wFlush()
            throws IOException {
        try {
            writer.finalizeChain(reader.getCurrChainState(), out);
        } catch (DRDAProtocolException dpe) {
            throw wrap(dpe);
        }
        writer.reset(null);
    }

    /* DDMReader forwarding methods */

    public void rReadReplyDss()
            throws IOException {
        try {
            reader.readReplyDss();
        } catch (DRDAProtocolException dpe) {
            throw wrap(dpe);
        }
    }

    public void rSkipDss()
            throws IOException {
        try {
            reader.readReplyDss();
            reader.skipDss();
        } catch (DRDAProtocolException dpe) {
            throw wrap(dpe);
        }
    }

    public void rSkipDdm()
            throws IOException {
        try {
            reader.readLengthAndCodePoint(false);
            reader.skipBytes();
        } catch (DRDAProtocolException dpe) {
            throw wrap(dpe);
        }
    }

    public void rSkipBytes()
            throws IOException {
        try {
            reader.skipBytes();
        } catch (DRDAProtocolException dpe) {
            throw wrap(dpe);
        }
    }

    public boolean rMoreData() {
        return reader.moreData();
    }

    public boolean rMoreDssData() {
        return reader.moreDssData();
    }

    public boolean rMoreDdmData() {
        return reader.moreDssData();
    }

    public int rReadNetworkShort()
            throws IOException {
        try {
            return reader.readNetworkShort();
        } catch (DRDAProtocolException dpe) {
            throw wrap(dpe);
        }
    }

    public byte rReadByte()
            throws IOException {
        try {
            return reader.readByte();
        } catch (DRDAProtocolException dpe) {
            throw wrap(dpe);
        }
    }

    public byte[] rReadBytes()
            throws IOException {
        try {
            return reader.readBytes();
        } catch (DRDAProtocolException dpe) {
            throw wrap(dpe);
        }
    }

    public int rReadLengthAndCodePoint(boolean f)
            throws IOException {
        try {
            return reader.readLengthAndCodePoint(f);
        } catch (DRDAProtocolException dpe) {
            throw wrap(dpe);
        }
    }

    public int rReadNetworkInt()
            throws IOException {
        try {
            return reader.readNetworkInt();
        } catch (DRDAProtocolException dpe) {
            throw wrap(dpe);
        }
    }

    public String rReadString(int length, String enc)
            throws IOException {
        try {
            return reader.readString(length, enc);
        } catch (DRDAProtocolException dpe) {
            throw wrap(dpe);
        }
    }

    /* Utility methods */

    /**
     * Wraps a protocol exception in a generic I/O exception, since
     * {@code DRDAProtocolException} is package private.
     */
    private static IOException wrap(DRDAProtocolException dpe) {
        IOException ioe = new IOException(dpe.getMessage());
        ioe.initCause(dpe);
        return ioe;
    }
}
