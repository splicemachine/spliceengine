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
package com.splicemachine.orc.metadata;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.Test;
import static org.junit.Assert.*;

import static com.splicemachine.orc.metadata.OrcMetadataReader.*;
import static java.lang.Character.MAX_CODE_POINT;
import static java.lang.Character.MIN_CODE_POINT;

public class TestOrcMetadataReader
{
    @Test
    public void testGetMinSlice()
            throws Exception
    {
        int startCodePoint = MIN_CODE_POINT;
        int endCodePoint = MAX_CODE_POINT;
        Slice minSlice = Slices.utf8Slice("");

        for (int i = startCodePoint; i < endCodePoint; i++) {
            String value = new String(new int[] { i }, 0, 1);
            if (firstSurrogateCharacter(value) == -1) {
                assertEquals(getMinSlice(value), Slices.utf8Slice(value));
            }
            else {
                assertEquals(getMinSlice(value), minSlice);
            }
        }

        // Test with prefix
        String prefix = "apple";
        for (int i = startCodePoint; i < endCodePoint; i++) {
            String value = prefix + new String(new int[] { i }, 0, 1);
            if (firstSurrogateCharacter(value) == -1) {
                assertEquals(getMinSlice(value), Slices.utf8Slice(value));
            }
            else {
                assertEquals(getMinSlice(value), Slices.utf8Slice(prefix));
            }
        }
    }

    @Test
    public void testGetMaxSlice()
            throws Exception
    {
        int startCodePoint = MIN_CODE_POINT;
        int endCodePoint = MAX_CODE_POINT;
        Slice maxByte = Slices.wrappedBuffer(new byte[] { (byte) 0xFF });

        for (int i = startCodePoint; i < endCodePoint; i++) {
            String value = new String(new int[] { i }, 0, 1);
            if (firstSurrogateCharacter(value) == -1) {
                assertEquals(getMaxSlice(value), Slices.utf8Slice(value));
            }
            else {
                assertEquals(getMaxSlice(value), maxByte);
            }
        }

        // Test with prefix
        String prefix = "apple";
        Slice maxSlice = concatSlices(Slices.utf8Slice(prefix), maxByte);
        for (int i = startCodePoint; i < endCodePoint; i++) {
            String value = prefix + new String(new int[] { i }, 0, 1);
            if (firstSurrogateCharacter(value) == -1) {
                assertEquals(getMaxSlice(value), Slices.utf8Slice(value));
            }
            else {
                assertEquals(getMaxSlice(value), maxSlice);
            }
        }
    }
}
