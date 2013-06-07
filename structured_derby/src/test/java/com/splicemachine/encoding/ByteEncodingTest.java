package com.splicemachine.encoding;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Random;

/**
 * @author Scott Fines
 * Created on: 6/10/13
 */
@RunWith(Parameterized.class)
public class ByteEncodingTest {
    private static final int numTests=100;
    private static final int arraysPerTest =100;
    private static final int bytesPerArray = 100;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Random random = new Random();
        Collection<Object[]> params = Lists.newArrayListWithCapacity(numTests);
        for(int test=0;test<numTests;test++){
            byte[][] data = new byte[arraysPerTest][];
            for(int array=0;array<data.length;array++){
                byte[] elem = new byte[bytesPerArray];
                random.nextBytes(elem);
                data[array] = elem;
            }
            params.add(new Object[]{data});
        }
        return params;
    }

    private final byte[][] data;

    public ByteEncodingTest(byte[][] data) {
        this.data = data;
    }

    @Test
    public void testCanSerializeAndDeserializeCorrectly() throws Exception {
        for(byte[] datum:data){
            byte[] serialized = ByteEncoding.encode(datum,false);
            byte[] decoded = ByteEncoding.decode(serialized,false);

            Assert.assertArrayEquals("incorrect encoding for element "+ Arrays.toString(datum),datum,decoded);
        }
    }


    @Test
    public void testSortOrderCorrect() throws Exception {
        byte[][] encoded = new byte[data.length][];
        for(int pos=0;pos<encoded.length;pos++){
            encoded[pos] = ByteEncoding.encode(data[pos],false);
        }

        Arrays.sort(encoded, Bytes.BYTES_COMPARATOR);

        byte[][] decoded = new byte[encoded.length][];
        for(int pos=0;pos<encoded.length;pos++){
            decoded[pos] = ByteEncoding.decode(encoded[pos],false);
        }

        Arrays.sort(data,Bytes.BYTES_COMPARATOR);
        Assert.assertArrayEquals("Incorrect sort order!",data,decoded);
    }

    @Test
    public void testReverseSortOrderCorrect() throws Exception {
        byte[][] encoded = new byte[data.length][];
        for(int pos=0;pos<encoded.length;pos++){
            encoded[pos] = ByteEncoding.encode(data[pos],true);
        }

        Arrays.sort(encoded, Bytes.BYTES_COMPARATOR);

        byte[][] decoded = new byte[encoded.length][];
        for(int pos=0;pos<encoded.length;pos++){
            decoded[pos] = ByteEncoding.decode(encoded[pos],true);
        }

        //sort original array in reverse
        Arrays.sort(data,new Comparator<byte[]>() {
            @Override
            public int compare(byte[] o1, byte[] o2) {
                return -1*Bytes.BYTES_COMPARATOR.compare(o1,o2);
            }
        });
        Assert.assertArrayEquals("Incorrect sort order!",data,decoded);
    }
}
