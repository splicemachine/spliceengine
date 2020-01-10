/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */
package com.google.protobuf;  // This is a lie.

/**
 * Helper class to extract byte arrays from {@link ByteString} without copy.
 * <p>
 * Without this protobufs would force us to copy every single byte array out
 * of the objects de-serialized from the wire (which already do one copy, on
 * top of the copies the JVM does to go from kernel buffer to C buffer and
 * from C buffer to JVM buffer).
 * <p>
 * @since 1.5
 */
public final class ZeroCopyLiteralByteString extends LiteralByteString {

    /** Private constructor so this class cannot be instantiated. */
    private ZeroCopyLiteralByteString() {
        super(null);
        throw new UnsupportedOperationException("Should never be here.");
    }

    /**
     * Wraps a byte array in a {@link ByteString} without copying it.
     */
    public static ByteString wrap(final byte[] array) {
        return new LiteralByteString(array);
    }

    /**
     * Extracts the byte array from the given {@link ByteString} without copy.
     * @param buf A buffer from which to extract the array.  This buffer must be
     * actually an instance of a {@code LiteralByteString}.
     */
    public static byte[] zeroCopyGetBytes(final ByteString buf) {
        if (buf instanceof LiteralByteString) {
            return ((LiteralByteString) buf).bytes;
        }
        throw new UnsupportedOperationException("Need a LiteralByteString, got a "
                + buf.getClass().getName());
    }

}
