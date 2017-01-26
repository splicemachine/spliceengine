/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
