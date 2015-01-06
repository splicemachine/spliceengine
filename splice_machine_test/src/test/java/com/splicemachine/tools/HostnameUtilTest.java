package com.splicemachine.tools;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import static org.junit.Assert.assertFalse;

public class HostnameUtilTest {

    @Test
    public void test() {
        String hostname = HostnameUtil.getHostname();
        assertFalse(StringUtils.isBlank(hostname));
    }

}