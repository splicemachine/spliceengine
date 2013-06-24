package com.splicemachine.utils;

import com.google.common.collect.Sets;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Scott Fines
 * Created on: 6/21/13
 */
public class Snowflake {
    private static final int timestampBits = 41;
    private static final int timestampShift = Long.SIZE-timestampBits;
    private static final int workerIdBits = 12;
    private static final int workerIdShift = timestampShift-workerIdBits;

    private static final long COUNTER_MASK = 0x07ff; //looks like 0111 1111 1111 (e.g. 11 bits of 1s)

    private final AtomicLong lastTimestamp = new AtomicLong(0l);
    private AtomicInteger counter = new AtomicInteger(Short.MIN_VALUE); //only first workerIdBits bits used
    private final long machineId; //only the first counterBits bits are used;

    public Snowflake(short machineId) {
        this.machineId = (machineId &0x0fff)<<workerIdShift; //take the first 12 bits, then shift it over appropriately
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
            boolean timeSet = false;
            short count =0; //we know that count is set no matter what, this just makes the compiler happy
            while(!timeSet && numTries>0){
                timestamp = System.currentTimeMillis();
                long lastTime = lastTimestamp.get();
                if(timestamp < lastTime){
                    //wait for just a bit in the hopes it'll catch up
                    numTries--;
                    interrupted = waitMs();
                }else if(lastTime == timestamp){
                    //need to make sure we didn't loop around our counter--that would be a sign of
                    //case #1
                    boolean countSet=false;
                    while(!countSet){
                        int currentCount = counter.get();
                        if(currentCount>=Short.MAX_VALUE){
                            counter.compareAndSet(currentCount,Short.MIN_VALUE);
                            countSet=true;

                            //wait a bit, then try again with a new ms
                            interrupted = waitMs();
                            numTries--;
                        }else{
                            count = (short)(currentCount+1);
                            countSet = counter.compareAndSet(currentCount,count);
                            timeSet=countSet; //we have a count value, so we don't need to worry about setting an uptodate timestamp
                        }
                    }
                }else{
                    lastTimestamp.compareAndSet(lastTime,timestamp);
                    //if we lose out on the compareAndSet, someone else set a larger value, so
                    //we don't have to worry about it
                    timeSet=true;
                    //we don't care about the looped counter
                    boolean countSet=false;
                    while(!countSet){
                        int currentCount = counter.get();
                        if(currentCount>=Short.MAX_VALUE){
                            count= Short.MIN_VALUE;
                        }else
                            count = (short)(currentCount+1);
                        countSet = counter.compareAndSet(currentCount,count);
                    }
                }
            }
            if(numTries<0)
                throw new IllegalStateException("Unable to acquire a UUID after 10 tries, check System clock");

            //we now have timestamp and counter--can build the UUID

            long uuid = timestamp << timestampShift;

            //push the machine bits
            uuid |= machineId;

            //push the count bits
            uuid |= (count &COUNTER_MASK);
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
        Snowflake snowflake = new Snowflake((short) (1 << 11));
        Set<Long> existing = Sets.newHashSet();
        for(int i=0;i<1000;i++){
            long next = snowflake.nextUUID();
            if(existing.contains(next))
                throw new IllegalStateException("Duplicate UUID found!");
            System.out.println(pad(next));
        }
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
