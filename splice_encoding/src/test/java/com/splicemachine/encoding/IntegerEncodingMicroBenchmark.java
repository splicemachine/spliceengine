/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.encoding;

/**
 * @author Scott Fines
 *         Created on: 6/6/13
 */
@Deprecated
public class IntegerEncodingMicroBenchmark {
	/*
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
        double serializationTimeRatio = (double)numSerStats.getWallClockTime()/rowSerStats.getWallClockTime();
        System.out.printf("numericStats.time/rowKeyStats.time: %f%n",serializationTimeRatio);
        System.out.println("-----");

        System.out.printf("Deserialization comparison%n");
        Stats numDeStats = numericStats.getSecond();
        Stats rowDeStats = rowKeyStats.getSecond();
        double deserializationTimeRatio = (double)numDeStats.getWallClockTime()/rowDeStats.getWallClockTime();
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
            byte[] data = ScalarEncoding.writeLong(next, false);
            long end = System.nanoTime();
            accumulator.tick(1,end-start);
            sum+=data.length;

            start = System.nanoTime();
            int val = ScalarEncoding.readInt(data, false);
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
    */
}
