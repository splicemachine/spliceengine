package com.splicemachine.collections.hashtable;

import com.splicemachine.hash.Hash32;
import com.splicemachine.hash.HashFunctions;
import com.splicemachine.metrics.DisplayTime;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.metrics.Timer;
import com.splicemachine.primitives.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * @author Scott Fines
 *         Date: 10/8/14
 */
public class SimpleHashTableTest {

    @Test
    public void testCanAddEntryAndFindItAgainString() throws Exception {
        final Hash32 hashFunction = HashFunctions.murmur3(0);
        HashTable<String,Long> table = new BaseRobinHoodHashTable<String, Long>(16,0.9f) {
            @Override protected int hash(String key) { return hashFunction.hash(key); }
            @Override protected Long merge(Long newValue, Long existing) { return newValue; }
        };
        int size = (int)(0.85f*131072);
        assertCorrect(table,new Generator<String>() {
            @Override
            public String generateKey(long iteration) {
                return Long.toString(iteration);
            }
        },size);
    }

    @Test
    public void testCanAddEntryAndFindItAgainByteBuffer() throws Exception {
        final Hash32 hashFunction = HashFunctions.murmur3(0);
        HashTable<ByteBuffer,Long> table = new BaseRobinHoodHashTable<ByteBuffer, Long>(16,0.9f) {
            @Override protected int hash(ByteBuffer key) { return hashFunction.hash(key); }
            @Override protected Long merge(Long newValue, Long existing) { return newValue; }
        };
        int size = (int)(0.85f*131072);
        assertCorrect(table,new Generator<ByteBuffer>() {
            @Override
            public ByteBuffer generateKey(long iteration) {
                return ByteBuffer.wrap(Bytes.toBytes(iteration));
            }
        },size);
    }

    private static <K> void assertCorrect(HashTable<K, Long> table,Generator<K> dataGen,int size) {
//        int size = 12;
        for(long i=0;i< size;i++){
            table.put(dataGen.generateKey(i),i);
            Long value = table.get(dataGen.generateKey(i));
            Assert.assertNotNull("Could not find element for key " + i + " after put!", value);
            Assert.assertEquals("Incorrect returned value!", i, value.longValue());
            if((i & (i - 1)) == 0){
                int n = 1;
                while(n<=i){
                    n<<=1;
                }
                System.out.printf("size=%d, nextPowOf2=%d,loadFactor=%f%n",table.size(),n,table.load());
            }
        }
        int n = 1;
        while(n<=size){
            n<<=1;
        }
        System.out.printf("size=%d, nextPowOf2=%d,loadFactor=%f%n",table.size(),n,table.load());
        Assert.assertEquals("Incorrect size!",size,table.size());

        long val = 150414l;
        table.put(dataGen.generateKey(val),val);
        Long value = table.get(dataGen.generateKey(val));
        Assert.assertNotNull("Could not find element for key " + val + " after put!", value);
        Assert.assertEquals("Incorrect returned value!", val,value.longValue());
    }

    @Test
    public void testCanAddEntryAndFindItAgain() throws Exception {
        HashTable<Long,Long> table = new SimpleHashTable<Long, Long>();
        int size = (int)(0.85f*131072);
        for(long i=0;i< size;i++){
            table.put(i,i);
            Long value = table.get(i);
            Assert.assertNotNull("Could not find element after put!",value);
            Assert.assertEquals("Incorrect returned value!",i,value.longValue());
            if((i & (i - 1)) == 0){
                int n = 1;
                while(n<=i){
                    n<<=1;
                }
                System.out.printf("size=%d, nextPowOf2=%d,loadFactor=%f%n",table.size(),n,table.load());
            }
        }
        int n = 1;
        while(n<=size){
            n<<=1;
        }
        System.out.printf("size=%d, nextPowOf2=%d,loadFactor=%f%n",table.size(),n,table.load());
        Assert.assertEquals("Incorrect size!",size,table.size());
    }

    @Test
    public void testCanDeleteElementsAndTheyGoAway() throws Exception {
        HashTable<Long,Long> table = new SimpleHashTable<Long, Long>();
        int size = 100;
        for(long i =0;i<size;i++){
            table.put(i,i);
            if(i%2==0){
               table.remove(i);
            }
        }
        for(long i=0;i<10;i++){
            Long value = table.get(i);
            if(i%2==0){
                Assert.assertNull("Value for key "+ i+" is not null",value);
            }else{
                Assert.assertNotNull("Could not find element after put for key "+ i,value);
                Assert.assertEquals("Incorrect returned value for key " + i, i, value.longValue());
            }
        }
    }

    @Test
    public void testCanFindElementsWithHighLoadFactorByteBuffers() throws Exception {
        /*
         * We have to use ByteBuffers because otherwise, HashMap won't find elements that match
         */
        int size = (int)(0.88f*(1<<20));
        int numIterations = 1000000;
//        int numIterations = Integer.MAX_VALUE;
        System.out.printf("Testing with RobinHood table%n");
        Generator<ByteBuffer> generator = new Generator<ByteBuffer>() {
            @Override
            public ByteBuffer generateKey(long iteration) {
                return ByteBuffer.wrap(Bytes.toBytes(iteration));
            }
        };
        final Hash32 murmur = HashFunctions.murmur3(0);
        performanceAnalysis(new BaseRobinHoodHashTable<ByteBuffer, Long>(16,0.9f) {
//            @Override protected int hash(ByteBuffer key) { return key.hashCode(); }
            @Override protected int hash(ByteBuffer key) { return murmur.hash(key); }
            @Override protected Long merge(Long newValue, Long existing) { return newValue; }
        }, size, numIterations,generator);
        System.out.printf("----------%n");
        System.out.printf("Testing with java.util.HashMap%n");
        performanceAnalysis(new HashMap<ByteBuffer, Long>(16,0.9f),size,numIterations,generator);
    }

    @Test
    public void testCanFindElementsWithHighLoadFactorString() throws Exception {
        int size = (int)(0.88f*(1<<20));
        int numIterations = 10000000;
//        int numIterations = Integer.MAX_VALUE;
//        System.out.printf("Testing with RobinHood table%n");
        Generator<String> generator = new Generator<String>() {
            @Override
            public String generateKey(long iteration) {
                return Long.toString(iteration);
            }
        };
        performanceAnalysis(new BaseRobinHoodHashTable<String, Long>(16,0.9f) {
            @Override protected int hash(String key) { return sun.misc.Hashing.stringHash32(key); }
            @Override protected Long merge(Long newValue, Long existing) { return newValue; }
        }, size, numIterations,generator);
        System.out.printf("----------%n");
        System.out.printf("Testing with java.util.HashMap%n");
        performanceAnalysis(new HashMap<String, Long>(16,0.9f),size,numIterations,generator);
    }


    private static <K> void performanceAnalysis(Map<K, Long> table, int size, int numIterations, Generator<K> generator) {
        //load up the hash table with a very high load factor
        Timer timer = Metrics.newWallTimer();
        timer.startTiming();
        for(long i=0;i< size;i++){
            table.put(generator.generateKey(i),i);
        }
        timer.tick(size);

        long loadTimeNs = timer.getTime().getWallClockTime();

        //now perform a bunch of searches and see what happens
        Random random = new Random(0l);
        timer = Metrics.newWallTimer();
        timer.startTiming();
        for(int i=0;i<numIterations;i++){
            Long val = table.get(generator.generateKey(random.nextInt(size)));
            if((i&(i-1))==0){
                System.out.printf("----performed %d iterations with val %d%n",i,val); //make sure that the loop isn't removed
            }
        }
        timer.stopTiming();

        System.out.printf("Loaded %d records up in %f s %n",size,
                DisplayTime.NANOSECONDS.toSeconds(loadTimeNs));
        System.out.printf("Performed %d lookups in %f s %n",numIterations,
                DisplayTime.NANOSECONDS.toSeconds(timer.getTime().getWallClockTime()));
    }

    private interface Generator<K> {
        K generateKey(long iteration);
    }
}
