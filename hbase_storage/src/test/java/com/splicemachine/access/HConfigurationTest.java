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

package com.splicemachine.access;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Map;

import org.junit.Test;

import com.splicemachine.access.api.SConfiguration;

/**
 * Test getting and using SConfiguration from HConfiguration.
 */
public class HConfigurationTest {

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
        assertEquals("true", value);
    }
}
