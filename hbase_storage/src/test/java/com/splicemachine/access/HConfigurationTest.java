/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

package com.splicemachine.access;

import com.splicemachine.access.api.SConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Test getting and using SConfiguration from HConfiguration.
 */
public class HConfigurationTest {
    Configuration conf = new Configuration();
    HBaseConfigurationSource hconf = new HBaseConfigurationSource(conf);

    @Test
    public void testInit() throws Exception {
        SConfiguration config = HConfiguration.getConfiguration();
        String auth = config.getAuthentication();
        assertEquals("NON-NATIVE", auth);
    }
    @Test
    public void testPrefixMatch() throws Exception {
        SConfiguration config = HConfiguration.getConfiguration();
        Map<String, String> auths = config.prefixMatch("splice.authentication.*");

        String key = "splice.authentication";
        String value = auths.get(key);
        assertNotNull(value);
        assertEquals("NON-NATIVE", value);

        key = "splice.authentication.native.algorithm";
        value = auths.get(key);
        assertNotNull(value);
        assertEquals("SHA-512", value);

        key = "splice.authentication.native.create.credentials.database";
        value = auths.get(key);
        assertNotNull(value);
        assertEquals("false", value);
    }

    @Test
    public void testGetInt() throws Exception {
        conf.set("testGetIntPositive", "5");
        assertEquals(5, hconf.getInt("testGetIntPositive", 0));

        conf.set("testGetIntNegative", "-5");
        assertEquals(-5, hconf.getInt("testGetIntNegative", 1));

        conf.set("testGetIntMin", Integer.toString(Integer.MIN_VALUE));
        assertEquals(Integer.MIN_VALUE, hconf.getInt("testGetIntMin", 2));

        conf.set("testGetIntMax", Integer.toString(Integer.MAX_VALUE));
        assertEquals(Integer.MAX_VALUE, hconf.getInt("testGetIntMax", 3));

        conf.set("testGetIntHex", "0xA");
        assertEquals(10, hconf.getInt("testGetIntHex", 4));

        conf.set("testGetIntNotNumber", "bonjour");
        assertEquals(5, hconf.getInt("testGetIntNotNumber", 5));

        // testGetNotSetNumber
        assertEquals(6, hconf.getInt("testGetNotSetNumber", 6));
        assertEquals(6, hconf.getInt("testGetNotSetNumber", 6, 0, 20));

        conf.set("testGetIntOverflow", Long.toString((long)Integer.MAX_VALUE + 1));
        assertEquals(7, hconf.getInt("testGetIntOverflow", 7));

        conf.set("testGetIntBelowMin", "-5");
        assertEquals(0, hconf.getInt("testGetIntBelowMin", 8, 0, 20));

        conf.set("testGetIntAboveMax", "100");
        assertEquals(20, hconf.getInt("testGetIntAboveMax", 9,0, 20));
    }

    @Test
    public void testGetLong() throws Exception {
        conf.set("testGetLongPositive", "5");
        assertEquals(5, hconf.getLong("testGetLongPositive", 0));

        conf.set("testGetLongNegative", "-5");
        assertEquals(-5, hconf.getLong("testGetLongNegative", 1));

        conf.set("testGetLongMin", Long.toString(Long.MIN_VALUE));
        assertEquals(Long.MIN_VALUE, hconf.getLong("testGetLongMin", 2));

        conf.set("testGetLongMax", Long.toString(Long.MAX_VALUE));
        assertEquals(Long.MAX_VALUE, hconf.getLong("testGetLongMax", 3));

        conf.set("testGetLongHex", "0xA");
        assertEquals(10, hconf.getLong("testGetLongHex", 4));

        conf.set("testGetLongNotNumber", "bonjour");
        assertEquals(5, hconf.getLong("testGetLongNotNumber", 5));

        // testGetNotSetNumber
        assertEquals(6, hconf.getLong("testGetNotSetNumber", 6));
        assertEquals(6, hconf.getLong("testGetNotSetNumber", 6, 0, 20));

        conf.set("testGetLongBelowMin", "-5");
        assertEquals(0, hconf.getLong("testGetLongBelowMin", 7, 0, 20));

        conf.set("testGetLongAboveMax", "100");
        assertEquals(20, hconf.getLong("testGetLongAboveMax", 8, 0, 20));
    }

    @Test
    public void testGetDouble() throws Exception {
        conf.set("testGetDoublePositive", "5.0");
        assertEquals(5, hconf.getDouble("testGetDoublePositive", 0), 0.001);

        conf.set("testGetDoubleNegative", "-5.0");
        assertEquals(-5, hconf.getDouble("testGetDoubleNegative", 1), 0.001);

        conf.set("testGetDoubleMin", Double.toString(Double.MIN_VALUE));
        assertEquals(Double.MIN_VALUE, hconf.getDouble("testGetDoubleMin", 2), 0.001);

        conf.set("testGetDoubleMax", Double.toString(Double.MAX_VALUE));
        assertEquals(Double.MAX_VALUE, hconf.getDouble("testGetDoubleMax", 3), 0.001);

        conf.set("testGetDoubleNotNumber", "bonjour");
        assertEquals(5, hconf.getDouble("testGetDoubleNotNumber", 5), 0.001);

        // testGetNotSetNumber
        assertEquals(6, hconf.getDouble("testGetNotSetNumber", 6), 0.001);
        assertEquals(6, hconf.getDouble("testGetNotSetNumber", 6, 0, 20), 0.001);

        conf.set("testGetDoubleBelowMin", "-5.0");
        assertEquals(0, hconf.getDouble("testGetDoubleBelowMin", 7, 0, 20), 0.001);

        conf.set("testGetDoubleAboveMax", "100.0");
        assertEquals(20, hconf.getDouble("testGetDoubleAboveMax", 8, 0, 20), 0.001);
    }

    @Test
    public void testGetBoolean() throws Exception {
        conf.set("testGetBooleanTrue", "true");
        assertEquals(true, hconf.getBoolean("testGetBooleanTrue", false));

        conf.set("testGetBooleanTrueWeirdCase", "TrUe");
        assertEquals(true, hconf.getBoolean("testGetBooleanTrueWeirdCase", false));

        conf.set("testGetBooleanFalse", "false");
        assertEquals(false, hconf.getBoolean("testGetBooleanFalse", true));

        conf.set("testGetBooleanFalseWeirdCase", "FaLSe");
        assertEquals(false, hconf.getBoolean("testGetBooleanFalseWeirdCase", true));

        conf.set("testGetBooleanNotBooleanDefaultFalse", "bonjour");
        assertEquals(false, hconf.getBoolean("testGetBooleanNotBooleanDefaultFalse", false));

        conf.set("testGetBooleanNotBooleanDefaultTrue", "bonjour");
        assertEquals(true, hconf.getBoolean("testGetBooleanNotBooleanDefaultTrue", true));

        // testGetNotSetBoolean
        assertEquals(true, hconf.getBoolean("testGetBooleanNotSet", true));
        assertEquals(false, hconf.getBoolean("testGetBooleanNotSet", false));
    }

    @Test
    public void testGetString() throws Exception {
        conf.set("testGetString", "bonjour");
        assertEquals("bonjour", hconf.getString("testGetString", "au revoir"));

        conf.set("testGetStringAccent", "Café");
        assertEquals("Café", hconf.getString("testGetStringAccent", "au revoir"));

        conf.set("testGetStringJapanese", "今日は");
        assertEquals("今日は", hconf.getString("testGetStringJapanese", "au revoir"));

        // testGetStringNotSet
        assertEquals("au revoir", hconf.getString("testGetStringNotSet", "au revoir"));
    }
}
