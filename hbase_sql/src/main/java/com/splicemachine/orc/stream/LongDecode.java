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
package com.splicemachine.orc.stream;

import com.splicemachine.orc.OrcCorruptionException;
import com.splicemachine.orc.metadata.OrcType.OrcTypeKind;

import java.io.IOException;
import java.io.InputStream;

import static com.splicemachine.orc.metadata.OrcType.OrcTypeKind.*;
import static com.splicemachine.orc.stream.LongDecode.FixedBitSizes.*;

// This is based on the Apache Hive ORC code
public final class LongDecode
{
    private LongDecode()
    {
    }

    enum FixedBitSizes
    {
        ONE, TWO, THREE, FOUR, FIVE, SIX, SEVEN, EIGHT, NINE, TEN, ELEVEN, TWELVE,
        THIRTEEN, FOURTEEN, FIFTEEN, SIXTEEN, SEVENTEEN, EIGHTEEN, NINETEEN,
        TWENTY, TWENTY_ONE, TWENTY_TWO, TWENTY_THREE, TWENTY_FOUR, TWENTY_SIX,
        TWENTY_EIGHT, THIRTY, THIRTY_TWO, FORTY, FORTY_EIGHT, FIFTY_SIX, SIXTY_FOUR;
    }

    /**
     * Decodes the ordinal fixed bit value to actual fixed bit width value.
     */
    public static int decodeBitWidth(int n)
    {
        if (n >= ONE.ordinal() && n <= TWENTY_FOUR.ordinal()) {
            return n + 1;
        }
        else if (n == TWENTY_SIX.ordinal()) {
            return 26;
        }
        else if (n == TWENTY_EIGHT.ordinal()) {
            return 28;
        }
        else if (n == THIRTY.ordinal()) {
            return 30;
        }
        else if (n == THIRTY_TWO.ordinal()) {
            return 32;
        }
        else if (n == FORTY.ordinal()) {
            return 40;
        }
        else if (n == FORTY_EIGHT.ordinal()) {
            return 48;
        }
        else if (n == FIFTY_SIX.ordinal()) {
            return 56;
        }
        else {
            return 64;
        }
    }

    /**
     * Gets the closest supported fixed bit width for the specified bit width.
     */
    public static int getClosestFixedBits(int width)
    {
        if (width == 0) {
            return 1;
        }

        if (width >= 1 && width <= 24) {
            return width;
        }
        else if (width > 24 && width <= 26) {
            return 26;
        }
        else if (width > 26 && width <= 28) {
            return 28;
        }
        else if (width > 28 && width <= 30) {
            return 30;
        }
        else if (width > 30 && width <= 32) {
            return 32;
        }
        else if (width > 32 && width <= 40) {
            return 40;
        }
        else if (width > 40 && width <= 48) {
            return 48;
        }
        else if (width > 48 && width <= 56) {
            return 56;
        }
        else {
            return 64;
        }
    }

    public static long readSignedVInt(InputStream inputStream)
            throws IOException
    {
        long result = readUnsignedVInt(inputStream);
        return (result >>> 1) ^ -(result & 1);
    }

    public static long readUnsignedVInt(InputStream inputStream)
            throws IOException
    {
        long result = 0;
        int offset = 0;
        long b;
        do {
            b = inputStream.read();
            if (b == -1) {
                throw new OrcCorruptionException("EOF while reading unsigned vint");
            }
            result |= (b & 0b0111_1111) << offset;
            offset += 7;
        } while ((b & 0b1000_0000) != 0);
        return result;
    }

    public static long readVInt(boolean signed, InputStream inputStream)
            throws IOException
    {
        if (signed) {
            return readSignedVInt(inputStream);
        }
        else {
            return readUnsignedVInt(inputStream);
        }
    }

    public static long zigzagDecode(long value)
    {
        return (value >>> 1) ^ -(value & 1);
    }

    public static long readDwrfLong(InputStream input, OrcTypeKind type, boolean signed, boolean usesVInt)
            throws IOException
    {
        if (usesVInt) {
            return readVInt(signed, input);
        }
        else if (type == SHORT) {
            return input.read() | (input.read() << 8);
        }
        else if (type == INT) {
            return input.read() | (input.read() << 8) | (input.read() << 16) | (input.read() << 24);
        }
        else if (type == LONG) {
            return ((long) input.read()) |
                    (((long) input.read()) << 8) |
                    (((long) input.read()) << 16) |
                    (((long) input.read()) << 24) |
                    (((long) input.read()) << 32) |
                    (((long) input.read()) << 40) |
                    (((long) input.read()) << 48) |
                    (((long) input.read()) << 56);
        }
        else {
            throw new IllegalArgumentException(type + " type is not supported");
        }
    }
}
