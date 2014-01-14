package com.splicemachine.constants.test;

import com.splicemachine.constants.bytes.BytesUtil;
import org.apache.hadoop.hbase.util.Pair;
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

    private void checkIntersect(byte[] a, byte[] b, byte[] x, byte[] y, byte[] r1, byte[] r2) {
        checkIntersectOneWay(a, b, x, y, r1, r2);
        checkIntersectOneWay(x, y, a, b, r1, r2);
    }

    private void checkIntersectOneWay(byte[] a, byte[] b, byte[] x, byte[] y, byte[] r1, byte[] r2) {
        final Pair<byte[],byte[]> intersect = BytesUtil.intersect(a, b, x, y);
        Assert.assertArrayEquals(r1, intersect.getFirst());
        Assert.assertArrayEquals(r2, intersect.getSecond());
    }

    private void checkIntersect(byte[] a, byte[] b, byte[] x, byte[] y) {
        checkIntersectOneWay(a, b, x, y);
        checkIntersectOneWay(x, y, a, b);
    }

    private void checkIntersectOneWay(byte[] a, byte[] b, byte[] x, byte[] y) {
        final Pair<byte[],byte[]> intersect = BytesUtil.intersect(a, b, x, y);
        Assert.assertNull(intersect);
    }

    @Test
    public void intersect() {
        checkIntersect(new byte[] {3}, new byte[] {5}, new byte[] { }, new byte[] { }, new byte[] {3}, new byte[] {5}); // _AA_
        checkIntersect(new byte[] {3}, new byte[] {5}, new byte[] { }, new byte[] { }, new byte[] {3}, new byte[] {5}); // _AA_
        checkIntersect(new byte[] {3}, new byte[] {5}, new byte[] { }, new byte[] {7}, new byte[] {3}, new byte[] {5}); // _AAX
        checkIntersect(new byte[] {3}, new byte[] {8}, new byte[] { }, new byte[] {5}, new byte[] {3}, new byte[] {5}); // _AXA
        checkIntersect(new byte[] { }, new byte[] {5}, new byte[] {7}, new byte[] {9});                                 // _AXX
        checkIntersect(new byte[] {5}, new byte[] {7}, new byte[] { }, new byte[] {4});                                 // _XAA
        checkIntersect(new byte[] { }, new byte[] {5}, new byte[] {2}, new byte[] {7}, new byte[] {2}, new byte[] {5}); // _XAX
        checkIntersect(new byte[] { }, new byte[] { }, new byte[] {2}, new byte[] {3}, new byte[] {2}, new byte[] {3}); // _XX_
        checkIntersect(new byte[] { }, new byte[] {9}, new byte[] {2}, new byte[] {3}, new byte[] {2}, new byte[] {3}); // _XXA
        checkIntersect(new byte[] {1}, new byte[] {5}, new byte[] {2}, new byte[] { }, new byte[] {2}, new byte[] {5}); // AXA_
        checkIntersect(new byte[] {1}, new byte[] {5}, new byte[] {2}, new byte[] {7}, new byte[] {2}, new byte[] {5}); // AXAX
        checkIntersect(new byte[] {1}, new byte[] { }, new byte[] {2}, new byte[] {3}, new byte[] {2}, new byte[] {3}); // AXX_
        checkIntersect(new byte[] {1}, new byte[] {9}, new byte[] {2}, new byte[] {3}, new byte[] {2}, new byte[] {3}); // AXXA
        checkIntersect(new byte[] {3}, new byte[] {5}, new byte[] {2}, new byte[] { }, new byte[] {3}, new byte[] {5}); // XAA_
        checkIntersect(new byte[] {3}, new byte[] {5}, new byte[] {2}, new byte[] {7}, new byte[] {3}, new byte[] {5}); // XAAX
        checkIntersect(new byte[] {3}, new byte[] { }, new byte[] {2}, new byte[] {5}, new byte[] {3}, new byte[] {5}); // XAX_
        checkIntersect(new byte[] {3}, new byte[] {8}, new byte[] {2}, new byte[] {5}, new byte[] {3}, new byte[] {5}); // XAXA
        checkIntersect(new byte[] {5}, new byte[] { }, new byte[] {2}, new byte[] {4});                                 // XXA_
        checkIntersect(new byte[] {5}, new byte[] {7}, new byte[] {2}, new byte[] {4});                                 // XXAA
        checkIntersect(new byte[] {3}, new byte[] {3}, new byte[] {2}, new byte[] {5}, new byte[] {3}, new byte[] {3}); // Checks for Equivalence : Bulk Loading
    }
    
    @Test
    public void intersectEquivalence() {
    	
    }

    @Test
    public void intersectCoincide() {
        checkIntersect(new byte[] {3}, new byte[] {5}, new byte[] {3}, new byte[] {5}, new byte[] {3}, new byte[] {5});
        checkIntersect(new byte[] {3}, new byte[] {5}, new byte[] {3}, new byte[] {4}, new byte[] {3}, new byte[] {4});
        checkIntersect(new byte[] {3}, new byte[] {5}, new byte[] {5}, new byte[] {7});
        checkIntersect(new byte[] { }, new byte[] {5}, new byte[] {5}, new byte[] { });
        checkIntersect(new byte[] { }, new byte[] {5}, new byte[] { }, new byte[] {5}, new byte[] { }, new byte[] {5});
        checkIntersect(new byte[] { }, new byte[] { }, new byte[] { }, new byte[] { }, new byte[] { }, new byte[] { });
        checkIntersect(new byte[] {2}, new byte[] { }, new byte[] {2}, new byte[] { }, new byte[] {2}, new byte[] { });
        checkIntersect(new byte[] {2}, new byte[] { }, new byte[] {6}, new byte[] { }, new byte[] {6}, new byte[] { });
        checkIntersect(new byte[] {1}, new byte[] {3}, new byte[] {2}, new byte[] {3}, new byte[] {2}, new byte[] {3});
    }


    @Test
    public void intersectLexicalOrdering() {
        checkIntersect(new byte[] {3, 0}, new byte[] {5}, new byte[] { }, new byte[] { }, new byte[] {3, 0}, new byte[] {5});
        checkIntersect(new byte[] {1, 0, 0, 0}, new byte[] {5, 0}, new byte[] {2, 0, 0}, new byte[] {7}, new byte[] {2, 0, 0}, new byte[] {5, 0});
        checkIntersect(new byte[] {1, 0, 0, 0}, new byte[] { }, new byte[] {2, 0, 0}, new byte[] {3, 0}, new byte[] {2, 0, 0}, new byte[] {3, 0});
        checkIntersect(new byte[] {3, 0, 0}, new byte[] {5, 2}, new byte[] {3, 0, 0}, new byte[] {5, 2}, new byte[] {3, 0, 0}, new byte[] {5, 2});
    }

}
