package com.splicemachine.utils;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * An algorithm for generating compact UUIDs efficiently.
 *
 * Snowflake itself is an algorithm designed at Twitter (https://github.com/twitter/snowflake),
 * which constructs a 64-bit UUID using:
 *
 * <ol>
 *    <li>41-bit timestamp--allows ~ 69 years of unique Milliseconds before repeating</li>
 *    <li>10-bit "configured machine id" --allows 1024 machines to generate ids concurrently</li>
 *    <li>12-bit counter -- allows 4096 UUIDs to be generated every millisecond</li>
 * </ol>
 *
 * This organization is roughly time-ordered, which was a requirement for Twitter's use case, but
 * is awkward for SpliceMachine (since random UUIDs would be preferable). Additionally, 1024
 * machines is close, but may in some (highly special) cases be insufficient.
 *
 * Thus, this is an adjusted Snowflake algorithm, using similar components, but in different order
 * (to ensure randomness), and with slightly adjusted sizes.
 *
 * First, one notices that 41+10+12 = 63 bits. When considering the actual implementation
 * of Snowflake, one notices that the sign bit on the long is not used. If that bit is used,
 * an additional bit is available for use. Adding that to the machine id allows for 12 bits,
 * which allows 4096 machines to concurrently run.
 *
 * Finally, to remove the sort order, placing the counter before the timer will essentially
 * bucket UUIDs into 4096 separate buckets, which gives a much faster roll-over rate on
 * UUIDs, causing order to be removed.
 *
 * @author Scott Fines
 * Created on: 6/21/13
 */
public class Snowflake {

    private static final long TIMESTAMP_MASK = 0x1ffffffffffl;
    private static final int timestampShift = 12;


    private static final short COUNTER_MASK = 0x07ff; //looks like 0111 1111 1111 (e.g. 11 bits of 1s)
    private static final long counterShift = 53;

    private volatile long lastTimestamp = 0l;
    private volatile short counter = 0;

    private final long machineId; //only the first counterBits bits are used;

    public Snowflake(short machineId) {
        /*
         * We want to use -1l as a "non entry" flag, but to do so we have to ensure that it can never occur naturally.
         * If -1 occurrs naturally, then we know that the counter looks like 0100 0000 0000 (e.g. 1024), and
         * both the timestamp and the machine id are 0l; Since it is possible for someone crazy to create a machine id of 0.0.0.0,
         * then reset all the system clocks to start counting from January 1, 1970, we have to watch out.
         *
         * The simplest way to do that is just to validate that we have a non-zero machine id
         */
        this.machineId = (machineId &0x0fff); //take the first 12 bits, then shift it over appropriately
        if(machineId==0)
            throw new IllegalArgumentException("Cannot have a machine id with all zeros!");
    }

    public byte[] nextUUIDBytes(){
        return Bytes.toBytes(nextUUID());
    }

    public void nextUUIDBlock(long[] uuids){
        boolean interrupted = false;
        try{
            //we now have time
            long timestamp;
            short startCount;
            short stopCount;
            int numRecords = uuids.length;
            synchronized (this) {
                timestamp = System.currentTimeMillis();
                if(timestamp<lastTimestamp)
                    throw new IllegalStateException("Unable to obtain timestamp, clock moved backwards");

                if(timestamp==lastTimestamp){
                    if(counter==0)
                        while(timestamp ==lastTimestamp)
                            timestamp = System.currentTimeMillis();

                    if(numRecords < COUNTER_MASK-counter+1){
                        startCount = counter;
                        counter+=numRecords;
                        stopCount = counter;
                        counter = (short)(counter & COUNTER_MASK);
                    }else{
                        numRecords = COUNTER_MASK-counter+1;
                        startCount = counter;
                        counter+=numRecords;
                        stopCount = counter;
                        counter = (short)(counter & COUNTER_MASK);
                    }
                }else{
                    //grab as many as you can, then return
                    if(numRecords < COUNTER_MASK-counter+1){
                        startCount = counter;
                        counter+=numRecords;
                        stopCount = counter;
                        counter = (short)(counter & COUNTER_MASK);
                    }else{
                        numRecords = COUNTER_MASK-counter+1;
                        startCount = counter;
                        counter+=numRecords;
                        stopCount = counter;
                        counter = (short)(counter & COUNTER_MASK);
                    }
                }
                lastTimestamp = timestamp;
            }
            for(int i=startCount;i<stopCount;i++){
                long uuid = ((long)i <<counterShift);
                uuid |= ((timestamp & TIMESTAMP_MASK)<<timestampShift);
                uuid |= machineId;
                uuids[i-startCount] = uuid;
            }
            for(int i=stopCount;i<uuids.length;i++){
                /*
                 * fill in any remaining entries with -1. We can safely do this because we know that
                 * machine ids are never all zeros, so we can't possibly have -1 as a valid snowflake number.
                 */
                uuids[i] = -1l;
            }
        }finally{
            if(interrupted){ //reset the interrupted flag for others to use
                Thread.currentThread().interrupt();
            }
        }
    }
    public long[] nextUUIDBlock(int numRecords){
        long[] uuids = new long[numRecords];
        nextUUIDBlock(uuids);
        return uuids;
    }

    public long nextUUID(){
//        return nextUUIDBlock(1)[0];
        boolean interrupted = false;
        try{
            long timestamp =0; //we know that timestamp is set, this just makes the compiler happy
            /*
             * Get the timestamp to use.
             *
             * There are several reasons why we have to check against the latest timestamp:
             *
             * 1. Prevent duplicate UUIDs because we ask for too many UUIDs too frequently (unlikely,
             * since that would be asking for more than 2048 UUIDs each millisecond.
             * 2. Protect against duplicate data in the event that the system clock ran backwards.
             *
             * In the first case, we merely have to wait for the next millisecond to roll over, but the
             * second case is more problematic. If we wait for the system clock to catch up to our latest timestamp,
             * we could be waiting for days (or more, if the system clock gets badly out of alignment). More likely,
             * we'd wait for 30 seconds, and then HBase would explode, but either way, we are waiting for too
             * long--we don't want to wait more than about 20ms before exploding. Thus, we retry 10 times, waiting
             * 2 ms in between each run.
             */
            int numTries=10;
            boolean timeSet=false;
            short count = counter;
            while(!timeSet &&numTries>0){
                /*
                 * When the counter goes over 11 bits, it needs to be reset. If that happens before the
                 * millisecond count can roll over, then we must block ALL threads until the millisecond clock
                 * can roll over (or we'll get duplicate UUIDs). Thus, we have to synchronize here, instead
                 * of using a non-blocking algorithm.
                 */
                synchronized (this){
                    timestamp= System.currentTimeMillis();
                    if(timestamp < lastTimestamp){
                       interrupted = waitMs(); //everyone has to wait until the timestamps line up
                       numTries--;
                    }else if(timestamp == lastTimestamp){
                        counter++;
                        counter = (short)(counter & COUNTER_MASK);
                        if(counter==0){
                            //rolled over
                            interrupted = waitMs();
                        }else{
                            count = counter;
                            timeSet=true;
                        }
                    }else{
                        lastTimestamp=timestamp;
                        timeSet=true;
                        counter++;
                        counter = (short)(counter & COUNTER_MASK);
                        count = counter;
                    }
                }
            }

            if(numTries<0)
                throw new IllegalStateException("Unable to acquire a UUID after 10 tries, check System clock");

            //we now have time

            long uuid = (long)count <<counterShift;

            uuid |= ((timestamp & TIMESTAMP_MASK)<<timestampShift);
            uuid |= machineId;

            return uuid;
        }finally{
            if(interrupted){ //reset the interrupted flag for others to use
                Thread.currentThread().interrupt();
            }
        }
    }

    public Generator newGenerator(int blockSize){
        return new Generator(this,blockSize);
    }

    public static class Generator{
        private final Snowflake snowflake;
        private long[] currentUuids;
        private final int batchSize;
        private int currentPosition;

        private Generator(Snowflake snowflake, int batchSize) {
            this.snowflake = snowflake;
            this.batchSize = batchSize;
            this.currentPosition = batchSize+1;
            this.currentUuids = new long[batchSize];
        }

        public long next(){
            if(currentPosition>=currentUuids.length||currentUuids[currentPosition]==-1l)
                refill();
            int pos = currentPosition;
            currentPosition++;
            return currentUuids[pos];
        }

        public byte[] nextBytes(){
            return Bytes.toBytes(next());
        }

        private void refill(){
            snowflake.nextUUIDBlock(currentUuids);
            currentPosition=0;
        }
    }

    private boolean waitMs() {
        try{
            Thread.sleep(1);
        } catch (InterruptedException e) {
            return true;
        }
        return false;
    }

    public static void main(String... args) throws Exception{
        long count = (1l<<10)+1;
        System.out.println(pad(count));
        long uuid = count << 53;
        System.out.println(pad(uuid));

        long timestampMask = 0x1ffffffffffl;
        System.out.println(pad(timestampMask));
        long timestamp = System.currentTimeMillis();
        System.out.println(pad(timestamp));
        uuid |= ((timestamp & timestampMask)<<12);
        System.out.println(pad(uuid));

        long worker = (1l<<11)+1;
        System.out.println(pad(worker));
        uuid |= worker;
        System.out.println(pad(uuid));

    }

    private static String pad(long number){
        String binary = Long.toBinaryString(number);
        char[] zeros = new char[Long.numberOfLeadingZeros(number)];
        for(int i=0;i<zeros.length;i++){
            zeros[i] = '0';
        }
        return new String(zeros)+binary;
    }

}
