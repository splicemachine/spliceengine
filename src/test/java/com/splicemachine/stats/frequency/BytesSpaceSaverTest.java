package com.splicemachine.stats.frequency;

import com.splicemachine.hash.HashFunctions;
import com.splicemachine.primitives.Bytes;
import junit.framework.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * @author Scott Fines
 *         Date: 2/19/15
 */
public class BytesSpaceSaverTest {

    @Test
    public void testFrequentElementsCorrectWithNoEvictionBytes() throws Exception {
        BytesFrequencyCounter counter = new BytesSpaceSaver(Bytes.BASE_COMPARATOR,HashFunctions.murmur3(0),100);

        fillPowersOf2(counter,false);

//        checkMultiplesOf8(counter.frequentElements(2),false);
        checkMultiplesOf4(counter.frequentElements(4),false);
    }

    @Test
    public void testFrequentElementsCorrectWithNoEvictionBuffers() throws Exception {
        BytesFrequencyCounter counter = new BytesSpaceSaver(Bytes.BASE_COMPARATOR,HashFunctions.murmur3(0),100);

        fillPowersOf2(counter,true);

//        checkMultiplesOf8(counter.frequentElements(2),true);
        checkMultiplesOf4(counter.frequentElements(4),true);
    }

    @Test
    public void testFrequentElementsCorrectWithEvictionBytes() throws Exception {
        BytesFrequencyCounter counter = new BytesSpaceSaver(Bytes.BASE_COMPARATOR,HashFunctions.murmur3(0),3);

        fillPowersOf2(counter,false);

//        checkMultiplesOf8(counter.frequentElements(2),false);
        checkMultiplesOf4(counter.frequentElements(4),false);
    }

    @Test
    public void testFrequentElementsCorrectWithEvictionBuffers() throws Exception {
        BytesFrequencyCounter counter = new BytesSpaceSaver(Bytes.BASE_COMPARATOR,HashFunctions.murmur3(0),3);

        fillPowersOf2(counter,true);

//        checkMultiplesOf8(counter.frequentElements(2),true);
        checkMultiplesOf4(counter.frequentElements(4),true);
    }


    /******************************************************************************************************************/
    /*private helper methods*/

    private void checkMultiplesOf4(BytesFrequentElements fe, boolean useByteBuffers) {
        byte[] minEight = Bytes.toBytes(-8);
        byte[] minFour = Bytes.toBytes(-4);
        byte[] zero = Bytes.toBytes(0);
        byte[] one = Bytes.toBytes(1);
        byte[] four = Bytes.toBytes(4);
        byte[] eight = Bytes.toBytes(8);
        ByteBuffer minEightBuf;
        ByteBuffer minFourBuf;
        ByteBuffer zeroBuf;
        ByteBuffer oneBuf;
        ByteBuffer fourBuf;
        ByteBuffer eightBuf;

        if(useByteBuffers) {
            minEightBuf = ByteBuffer.wrap(minEight);
            minFourBuf = ByteBuffer.wrap(minFour);
            zeroBuf = ByteBuffer.wrap(zero);
            oneBuf = ByteBuffer.wrap(one);
            fourBuf = ByteBuffer.wrap(four);
            eightBuf = ByteBuffer.wrap(eight);
            Assert.assertEquals("Incorrect value for -8!", 15, fe.countEqual(minEightBuf).count());
            Assert.assertEquals("Incorrect value for -4!", 7, fe.countEqual(minFourBuf).count()-fe.countEqual(minFourBuf).error());
            Assert.assertEquals("Incorrect value for 0!", 15, fe.countEqual(zeroBuf).count());
            Assert.assertEquals("Incorrect value for 4!", 7, fe.countEqual(fourBuf).count()-fe.countEqual(fourBuf).error());
            Assert.assertEquals("Incorrect value for 1!", 0, fe.countEqual(oneBuf).count());
            Assert.assertEquals("Incorrect value for 8!", 0, fe.countEqual(eightBuf).count());
        }else{
            Assert.assertEquals("Incorrect value for -8!", 15, fe.countEqual(minEight).count());
            Assert.assertEquals("Incorrect value for -4!", 7, fe.countEqual(minFour).count()-fe.countEqual(minFour).error());
            Assert.assertEquals("Incorrect value for 0!", 15, fe.countEqual(zero).count());
            Assert.assertEquals("Incorrect value for 4!", 7, fe.countEqual(four).count()-fe.countEqual(four).error());
            Assert.assertEquals("Incorrect value for 1!", 0, fe.countEqual(one).count());
            Assert.assertEquals("Incorrect value for 8!", 0, fe.countEqual(eight).count());
        }
    }

    private void fillPowersOf2(BytesFrequencyCounter counter, boolean useByteBuffers) {
        for(int i=-8;i<8;i++){
            byte[] data = Bytes.toBytes(i);
            if(useByteBuffers){
                ByteBuffer buffer = ByteBuffer.wrap(data);
                counter.update(buffer);
                if(i%2==0)counter.update(buffer,2);
                if(i%4==0)counter.update(buffer,4);
                if(i%8==0)counter.update(buffer,8);
            }else{
                counter.update(data);
                if(i%2==0)counter.update(data,2);
                if(i%4==0)counter.update(data,4);
                if(i%8==0)counter.update(data,8);

            }
        }
    }
}
