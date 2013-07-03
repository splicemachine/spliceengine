package com.splicemachine.utils;

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
        this.machineId = (machineId &0x0fff); //take the first 12 bits, then shift it over appropriately
    }

    public long nextUUID(){
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
