/*
 * Copyright (c) 2012 - 2021 Splice Machine, Inc.
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
 *
 */

package com.splicemachine.qpt;

import org.junit.Assert;
import org.junit.Test;

public class CommonTest {
    public static String repeat(String s, int n) {
        return new String(new char[n]).replace("\0", s);
    }

    @Test
    public void testEncoding() {
        Assert.assertEquals("XjOWk700", Encoding.makeId("X", 123456789));
        Assert.assertEquals("U0000000", Encoding.makeId("U", 0));
        Assert.assertEquals("AZZZZZZZ", Encoding.makeId("A", -1));
    }
}
