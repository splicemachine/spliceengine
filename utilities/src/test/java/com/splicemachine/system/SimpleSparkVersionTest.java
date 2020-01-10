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

package com.splicemachine.system;


import org.junit.Test;
import static org.junit.Assert.*;

public class SimpleSparkVersionTest{

    @Test
    public void comparisonTests() {

        SimpleSparkVersion version = new SimpleSparkVersion("2.3.4.rainbow");
        assertEquals(2, version.getMajorVersionNumber());
        assertEquals(3, version.getMinorVersionNumber());
        assertEquals(4, version.getPatchVersionNumber());

        SimpleSparkVersion version2  = new SimpleSparkVersion("2.3.5");
        SimpleSparkVersion version3  = new SimpleSparkVersion("2.4.4");
        SimpleSparkVersion version3a = new SimpleSparkVersion("2.4.4.brite");
        SimpleSparkVersion version4  = new SimpleSparkVersion("3.0.0");
        assertTrue(version4.greaterThanOrEqualTo(version));
        assertTrue(version3.greaterThanOrEqualTo(version));
        assertTrue(version3a.greaterThanOrEqualTo(version));
        assertTrue(version2.greaterThanOrEqualTo(version));
        assertTrue(version4.greaterThanOrEqualTo(version2));
        assertTrue(version3.greaterThanOrEqualTo(version2));
        assertTrue(version3a.greaterThanOrEqualTo(version2));
        assertTrue(version4.greaterThanOrEqualTo(version3));
        assertTrue(version3a.greaterThanOrEqualTo(version3));
        assertTrue(version3a.equals(version3));

        assertFalse(version4.lessThan(version));
        assertFalse(version3.lessThan(version));
        assertFalse(version2.lessThan(version));
        assertFalse(version4.lessThan(version2));
        assertFalse(version3.lessThan(version2));
        assertFalse(version4.lessThan(version3));
    }

}
