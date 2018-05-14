package com.splicemachine.client;

import org.junit.Test;

import static org.junit.Assert.*;

public class SpliceClientTest {

    @Test
    public void testMaskJDBCPassword() {
        String raw = "No suitable driver found for jdbc:splice://dwbdtest1r1w3.wellpoint" +
                ".com:1527/splicedb;user=af29891;password=XXXXX";
        String masked = "No suitable driver found for jdbc:splice://dwbdtest1r1w3.wellpoint" +
                ".com:1527/splicedb;user=af29891;password=[Omitted]";
        assertEquals(masked, SpliceClient.maskJDBCPassword(raw));

        raw = "No suitable driver found for jdbc:splice://dwbdtest1r1w3.wellpoint" +
                ".com:1527/splicedb;password=XXXXX;user=af29891";
        masked = "No suitable driver found for jdbc:splice://dwbdtest1r1w3.wellpoint" +
                ".com:1527/splicedb;password=[Omitted];user=af29891";
        assertEquals(masked, SpliceClient.maskJDBCPassword(raw));
    }
}
