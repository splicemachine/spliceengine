/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
