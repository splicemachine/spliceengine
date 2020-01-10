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

package com.splicemachine.primitives;

import org.junit.Test;
import java.util.Random;
import static org.junit.Assert.*;

/**
 * Created by jleach on 11/11/15.
 */
public class BytesTest {

    @Test
    public void toHex_fromHex() {
        byte[] bytesIn = new byte[1024];
        new Random().nextBytes(bytesIn);

        String hex = Bytes.toHex(bytesIn);
        byte[] bytesOut = Bytes.fromHex(hex);

        assertArrayEquals(bytesIn, bytesOut);
    }

    @Test
    public void bytesToLong() {
        long longIn = new Random().nextLong();
        byte[] bytes = Bytes.longToBytes(longIn);
        long longOut = Bytes.bytesToLong(bytes, 0);
        assertEquals(longIn, longOut);
    }
}
