package com.splicemachine.encoding;

import com.gotometrics.orderly.LongRowKey;
import com.gotometrics.orderly.RowKey;
import com.splicemachine.derby.stats.Accumulator;
import com.splicemachine.derby.stats.Stats;
import com.splicemachine.derby.stats.TimingStats;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Ignore;

import java.io.IOException;
import java.util.Random;

/**
 * @author Scott Fines
 *         Created on: 6/6/13
 */
@Ignore
public class IntegerEncodingMicroBenchmark {
    private static final int numSerializations = 1000000;

    public static void main(String...args) throws IOException {

        System.out.printf("Benchmarking LongRowKey%n");
        Pair<Stats,Stats> rowKeyStats = benchmarkRowKey();
        System.out.println("-----");

        System.out.printf("Benchmarking ScalarEncoding%n");
        Pair<Stats,Stats> numericStats = benchmarkNumericEncoder();
        System.out.println("-----");

        System.out.printf("Serialization comparison%n");
        Stats numSerStats = numericStats.getFirst();
        Stats rowSerStats = rowKeyStats.getFirst();
        double serializationTimeRatio = (double)numSerStats.getTotalTime()/rowSerStats.getTotalTime();
        System.out.printf("numericStats.time/rowKeyStats.time: %f%n",serializationTimeRatio);
        System.out.println("-----");

        System.out.printf("Deserialization comparison%n");
        Stats numDeStats = numericStats.getSecond();
        Stats rowDeStats = rowKeyStats.getSecond();
        double deserializationTimeRatio = (double)numDeStats.getTotalTime()/rowDeStats.getTotalTime();
        System.out.printf("numericStats.time/rowKeyStats.time: %f%n",deserializationTimeRatio);
        System.out.println("-----");
    }

    private static Pair<Stats,Stats> benchmarkNumericEncoder() throws IOException {
        Random random = new Random();
        long sum =0l;
        long otherSum=0l;
        Accumulator accumulator = TimingStats.uniformAccumulator();
        accumulator.start();
        Accumulator deAccum = TimingStats.uniformAccumulator();
        deAccum.start();
        for(int i=0;i<numSerializations;i++){
            int next = random.nextInt();
            long start = System.nanoTime();
            byte[] data = ScalarEncoding.toBytes(next, false);
            long end = System.nanoTime();
            accumulator.tick(1,end-start);
            sum+=data.length;

            start = System.nanoTime();
            int val = ScalarEncoding.getInt(data, false);
            end = System.nanoTime();
            deAccum.tick(1,end-start);
            otherSum+=val;
        }
        Stats finish = accumulator.finish();
        Stats deFinish = deAccum.finish();
        //print this out so that the loop doesn't get optimized away
        System.out.printf("sum=%d%n",sum);
        System.out.printf("otherSum=%d%n",otherSum);
        System.out.println(finish);
        System.out.println(deFinish);

        return Pair.newPair(finish,deFinish);
    }

    private static Pair<Stats,Stats> benchmarkRowKey() throws IOException {
        Random random = new Random();
        RowKey rowKey = new LongRowKey();
        long sum =0l;
        long otherSum=0l;
        Accumulator accumulator = TimingStats.uniformAccumulator();
        accumulator.start();

        Accumulator deAccum = TimingStats.uniformAccumulator();
        deAccum.start();
        for(int i=0;i<numSerializations;i++){
            int next = random.nextInt();
            long start = System.nanoTime();
            byte[] data = rowKey.serialize((long)next);
            long end = System.nanoTime();
            accumulator.tick(1,end-start);
            sum+=data.length;

            start = System.nanoTime();
            int val = ((Long)rowKey.deserialize(data)).intValue();
            end = System.nanoTime();
            deAccum.tick(1,end-start);
            otherSum+=val;
        }
        Stats finish = accumulator.finish();
        Stats deFinish = deAccum.finish();
        //print this out so that the loop doesn't get optimized away
        System.out.printf("sum=%d%n",sum);
        System.out.printf("otherSum=%d%n",otherSum);
        System.out.println(finish);
        System.out.println(deFinish);
        return Pair.newPair(finish,deFinish);
    }
}
