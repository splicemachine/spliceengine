package com.splicemachine.utils;

import com.splicemachine.constants.bytes.BytesUtil;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;
import java.util.Date;

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
		private static final long TIMESTAMP_LOCATION = (TIMESTAMP_MASK<<timestampShift);


    private static final short COUNTER_MASK = 0x07ff; //looks like 0111 1111 1111 (e.g. 11 bits of 1s)
    private static final long counterShift = 53;
		/**
		 * Fixed constant to allow consumers of Snowflake UUIDs to allow space in byte[] for
		 * UUIDs without knowing the exact length of the UUID.
		 */
		public static final int UUID_BYTE_SIZE = 8;

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

		/**
		 * Get the timestamp portion from the UUID.
		 *
		 * Recall that the UUID contains only 41 bits of the actual timestamp. This goes until Sep. 7, 2039,
		 * so be aware.
		 *
		 * @param uuid the uuid to get the timestamp from
		 * @return the timestamp portion of the UUID.
		 */
		public static long timestampFromUUID(long uuid){
			return (uuid & TIMESTAMP_LOCATION)>>timestampShift;
		}

    public byte[] nextUUIDBytes(){
        return Bytes.toBytes(nextUUID());
    }

    public void nextUUIDBlock(long[] uuids){
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
            }

            if(numRecords> COUNTER_MASK-counter+1){
                numRecords = COUNTER_MASK-counter+1;
            }
            startCount = counter;
            counter+=numRecords;
            stopCount = counter;
            counter = (short)(counter & COUNTER_MASK);
            lastTimestamp = timestamp;
        }
        int pos =0;
        //noinspection ConstantConditions
        for(int i=startCount;i<stopCount;i++,pos++){
            uuids[pos] = buildUUID(timestamp,i);
        }
            /*
             * fill in any remaining entries with -1. We can safely do this because we know that
             * machine ids are never all zeros, so we can't possibly have -1 as a valid snowflake number.
             */
        if(pos<uuids.length)
            Arrays.fill(uuids,stopCount-startCount,uuids.length,-1l);
    }

    public long nextUUID(){
        long timestamp;
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
        short count;
                /*
                 * When the counter goes over 11 bits, it needs to be reset. If that happens before the
                 * millisecond count can roll over, then we must block ALL threads until the millisecond clock
                 * can roll over (or we'll get duplicate UUIDs). Thus, we have to synchronize here, instead
                 * of using a non-blocking algorithm.
                 */
        synchronized (this){
            timestamp = System.currentTimeMillis();
            if(timestamp<lastTimestamp)
                throw new IllegalStateException("Unable to obtain timestamp, clock moved backwards");

            if(timestamp==lastTimestamp){
                if(counter==0)
                    while(timestamp ==lastTimestamp)
                        timestamp = System.currentTimeMillis();
            }

            count = counter;
            counter++;
            counter = (short)(counter & COUNTER_MASK);
            lastTimestamp = timestamp;
        }

        if(numTries<0)
            throw new IllegalStateException("Unable to acquire a UUID after 10 tries, check System clock");

        //we now have time

        return buildUUID(timestamp, count);
    }

    private long buildUUID(long timestamp, long count) {
        long uuid = count <<counterShift;

        uuid |= ((timestamp & TIMESTAMP_MASK)<<timestampShift);
        uuid |= machineId;
        return uuid;
    }

    public Generator newGenerator(int blockSize){
        /*
         * For correctness, we need to make sure that the Generator's block
         * size is a power of 2. So, find the smallest power of 2 large enough
         * to hold the block size in
         */
        int s = 1;
        while(s<blockSize){
            s<<=1;
        }
        return new Generator(this,s);
    }

    public static class Generator implements UUIDGenerator{
        private final Snowflake snowflake;
        private long[] currentUuids;
        private int currentPosition;

        private Generator(Snowflake snowflake, int batchSize) {
            this.snowflake = snowflake;
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

				@Override public byte[] nextBytes(){ return Bytes.toBytes(next()); }
				@Override public int encodedLength() { return 8; }

				@Override
				public void next(byte[] data, int offset) {
						long nextLong = next();
						BytesUtil.longToBytes(nextLong,data,offset);
				}

				private void refill(){
            snowflake.nextUUIDBlock(currentUuids);
            currentPosition=0;
        }
    }

    public static void main(String... args) throws Exception{
				long[] toDecode = new long[]{
								-2074918693534679039l,
								-7920591009858039807l,
								-903982790418145279l,
								-9091526912974639103l,
								2320594542789664769l,
								-6857741497818464255l,
								-6875755896327991295l,
								-4533884090081972223l
				};

				for(long l:toDecode){
						System.out.println(timestampFromUUID(l));
				}
    }

    @SuppressWarnings("unused")
	private static String pad(long number){
        String binary = Long.toBinaryString(number);
        char[] zeros = new char[Long.numberOfLeadingZeros(number)];
        for(int i=0;i<zeros.length;i++){
            zeros[i] = '0';
        }
        return new String(zeros)+binary;
    }

}
