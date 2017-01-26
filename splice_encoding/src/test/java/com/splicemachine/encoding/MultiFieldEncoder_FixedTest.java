/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.encoding;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class MultiFieldEncoder_FixedTest {

    @Test
    public void encodeEmpty() {
        int FIELD_COUNT = 10;

        MultiFieldEncoder multiFieldEncoder = MultiFieldEncoder.create(FIELD_COUNT);

        for (int i = 0; i < FIELD_COUNT; i++) {
            multiFieldEncoder.encodeEmpty();
        }

        byte[] result = multiFieldEncoder.build();

        assertEquals(FIELD_COUNT - 1, result.length);
        assertArrayEquals(new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0}, result);
    }

    @Test
    public void encodeNext_withEmpty() {

        MultiFieldEncoder encoder1 = MultiFieldEncoder.create(3);
        encoder1.encodeNext("A");
        encoder1.encodeEmpty();
        encoder1.encodeNext("B");

        MultiFieldEncoder encoder2 = MultiFieldEncoder.create(3);
        encoder2.encodeNext("A");
        encoder2.encodeNext("B");

        MultiFieldEncoder encoder3 = MultiFieldEncoder.create(3);
        encoder3.encodeNext("A");
        encoder3.encodeNext("B");
        encoder3.encodeEmpty();

        MultiFieldEncoder encoder4 = MultiFieldEncoder.create(3);
        encoder4.encodeEmpty();
        encoder4.encodeNext("A");
        encoder4.encodeNext("B");

        assertArrayEquals(new byte[]{67, 0, 0, 68}, encoder1.build());
        assertArrayEquals(new byte[]{67, 0, 68}, encoder2.build());
        assertArrayEquals(new byte[]{67, 0, 68, 0}, encoder3.build());
        assertArrayEquals(new byte[]{0, 67, 0, 68}, encoder4.build());

    }

    @Test
    public void encodeNext_withNull() {

        MultiFieldEncoder encoder1 = MultiFieldEncoder.create(3);
        encoder1.encodeNext("A");
        encoder1.encodeNext((String)null);
        encoder1.encodeNext("B");

        MultiFieldEncoder encoder2 = MultiFieldEncoder.create(3);
        encoder2.encodeNext("A");
        encoder2.encodeNext("B");

        MultiFieldEncoder encoder3 = MultiFieldEncoder.create(3);
        encoder3.encodeNext("A");
        encoder3.encodeNext("B");
        encoder3.encodeNext((String) null);

        MultiFieldEncoder encoder4 = MultiFieldEncoder.create(3);
        encoder4.encodeNext((String) null);
        encoder4.encodeNext("A");
        encoder4.encodeNext("B");

        assertArrayEquals(new byte[]{67, 0, 0, 68}, encoder1.build());
        assertArrayEquals(new byte[]{67, 0, 68}, encoder2.build());
        assertArrayEquals(new byte[]{67, 0, 68, 0}, encoder3.build());
        assertArrayEquals(new byte[]{0, 67, 0, 68}, encoder4.build());

    }


}
