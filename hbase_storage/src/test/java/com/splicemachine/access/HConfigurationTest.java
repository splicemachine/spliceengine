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
