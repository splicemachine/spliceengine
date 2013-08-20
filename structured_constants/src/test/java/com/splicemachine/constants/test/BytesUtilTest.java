package com.splicemachine.constants.test;

import com.splicemachine.constants.bytes.BytesUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class BytesUtilTest {

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{
                        new int[]{Integer.MIN_VALUE, -1000000000, -100000000, -10000000, -1000000, -100000, -10000, -1000, -100, -10, -1, 0}
                },
                new Object[]{
                        new int[]{2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384}
                },
                new Object[]{
                        new int[]{-2,-4, -8, -16, -32, -64, -128, -256, -512, -1024, -2048, -4096, -8192, -16384}
                },
                new Object[]{
                        new int[]{1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000, Integer.MAX_VALUE}
                }
        );
    }

    private final int[] intsToTest;

    public BytesUtilTest(int[] intsToTest) {
        this.intsToTest = intsToTest;
    }

    @Test
    public void testCanEncodeAndDecodeIntegersCorrectly() throws Exception {
        for(int toTest:intsToTest){
            byte[] data = new byte[4];
            BytesUtil.intToBytes(toTest, data, 0);

            int decoded = BytesUtil.bytesToInt(data,0);
            Assert.assertEquals("Incorrect decoded value!",toTest,decoded);
        }

    }
}
