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
    private static final short MAX_COUNTER_VALUE = 0x07ff; //11 bits of 1s,

    private volatile long lastTimestamp = 0l;
    private volatile short counter = 0;

    private final long machineId; //only the first counterBits bits are used;


    private volatile boolean looped = false;

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
            boolean timeSet=false;
            short count = counter;
            while(!timeSet &&numTries>0){
                synchronized (this){
                    long time= System.currentTimeMillis();
                    if(time < lastTimestamp){
                       interrupted = waitMs(); //everyone has to wait until the times line up
                       numTries--;
                    }else if(time == lastTimestamp){
                        if(counter>=MAX_COUNTER_VALUE){
                            counter =0;
                            count = counter;
                            interrupted = waitMs(); //everyone has to wait until we've pushed to a new  timestamp
                        }else{
                            count = counter++;
                            timeSet=true;
                        }
                    }else{
                        timeSet=true;
                        if(counter>=MAX_COUNTER_VALUE){
                            count = counter = 0;
                        }else
                            count = counter++;
                    }
                }
            }

            if(numTries<0)
                throw new IllegalStateException("Unable to acquire a UUID after 10 tries, check System clock");

            //we now have timestamp and counter--can build the UUID

            long uuid = timestamp << timestampShift;
//            System.out.println("          "+ Long.toBinaryString(uuid));

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
        System.out.println(pad(COUNTER_MASK));

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
