package com.splicemachine.utils;

import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.splicemachine.utils.hash.MurmurHash;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Random;

/**
 * @author Scott Fines
 *         Created on: 11/2/13
 */
@RunWith(Parameterized.class)
public class MurmurHashTest {
    private static final int maxRuns=1000;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> data = Lists.newArrayListWithCapacity(100);
        Random random = new Random(0l);
        for(int i=0;i<maxRuns;i++){
            byte[] dataPoint = new byte[i];
            random.nextBytes(dataPoint);
            data.add(new Object[]{dataPoint});
        }
        return data;
    }

    private final byte[] sampleData;

    public MurmurHashTest(byte[] sampleData) {
        this.sampleData = sampleData;
    }

    @Test
    public void testMatchesGoogleVersionMurmur332() throws Exception {
        HashCode hashCode = Hashing.murmur3_32(0).hashBytes(sampleData, 0, sampleData.length);
        int actual =hashCode.asInt();

        int hash = MurmurHash.murmur3_32(sampleData, 0, sampleData.length, 0);

        Assert.assertEquals(actual,hash);
    }
}
