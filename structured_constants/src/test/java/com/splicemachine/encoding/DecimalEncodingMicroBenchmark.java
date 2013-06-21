package com.splicemachine.encoding;


/**
 * @author Scott Fines
 *         Created on: 6/7/13
 */
@Deprecated
public class DecimalEncodingMicroBenchmark {
/*
    private static final int numSerializations = 100000000;
    private static final int maxBitsPerBigInteger = 100;

    public static void main(String... args) throws Exception{
        System.out.printf("Benchmarking NumericEncoder%n");
        Pair<Stats,Stats> numericStats = benchmarkNumericEncoder();
        System.out.println("-----");

        System.out.printf("Benchmarking LongRowKey%n");
        Pair<Stats,Stats> rowKeyStats = benchmarkRowKey();
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

    private static Pair<Stats, Stats> benchmarkNumericEncoder() {
        Random random = new Random();
        long sum =0l;
        long otherSum=0l;
        Accumulator accumulator = TimingStats.uniformAccumulator();
        accumulator.start();
        Accumulator deAccum = TimingStats.uniformAccumulator();
        deAccum.start();
        for(int i=0;i<numSerializations;i++){
            BigInteger next = new BigInteger(maxBitsPerBigInteger,random);
            BigDecimal n = new BigDecimal(next,random.nextInt(maxBitsPerBigInteger));
            long start = System.nanoTime();
            byte[] data = DecimalEncoding.toBytes(n,false);
            long end = System.nanoTime();
            accumulator.tick(1,end-start);
            sum+=data.length;

            start = System.nanoTime();
            BigDecimal val = DecimalEncoding.toBigDecimal(data,false);
            end = System.nanoTime();
            deAccum.tick(1,end-start);
            otherSum+=val.precision();
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

    private static Pair<Stats, Stats> benchmarkRowKey() throws IOException {
        Random random = new Random();
        long sum =0l;
        long otherSum=0l;
        RowKey rowKey = new BigDecimalRowKey();
        Accumulator accumulator = TimingStats.uniformAccumulator();
        accumulator.start();
        Accumulator deAccum = TimingStats.uniformAccumulator();
        deAccum.start();
        for(int i=0;i<numSerializations;i++){
            BigInteger next = new BigInteger(maxBitsPerBigInteger,random);
            BigDecimal n = new BigDecimal(next,random.nextInt(maxBitsPerBigInteger));
            long start = System.nanoTime();
            byte[] data = rowKey.serialize(n);
            long end = System.nanoTime();
            accumulator.tick(1,end-start);
            sum+=data.length;

            start = System.nanoTime();
            BigDecimal val = (BigDecimal)rowKey.deserialize(data);
            end = System.nanoTime();
            deAccum.tick(1,end-start);
            otherSum+=val.precision();
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

    private static String nums = "0123456789";
    private static final int numIterations = 1000;
    private static final int maxSizeForNumber = 10000;

    private static long benchmarkDecimalDivision() {
        Random random = new Random();
        long numDigits=0l;
        long totalTime=0l;
        for(int i=0;i<numIterations;i++){
            BigInteger val = randomInt(random);
            long start = System.nanoTime();
            int currSize = 10;
            BigInteger tenPow = BigInteger.TEN.pow(currSize);
            int decimalDigits = 1;
            while(currSize>=1){
                while(val.compareTo(tenPow)>0){
                    decimalDigits+=currSize;
                    val = val.divide(tenPow);
                }
                tenPow = tenPow.divide(BigInteger.TEN);
                currSize--;
            }
            long end = System.nanoTime();
            numDigits+=decimalDigits;
            totalTime+=(end-start);
        }
        System.out.println(numDigits);
        return totalTime;
    }

    private static long benchmarkStringCounting(){
        Random random = new Random();
        long numDigits=0l;
        long totalTime=0l;
        for(int i=0;i<numIterations;i++){
            BigInteger val = randomInt(random);
            long start = System.nanoTime();
            int decimalDigits = val.toString().length();
            long end = System.nanoTime();
            numDigits+=decimalDigits;
            totalTime+=(end-start);
        }
        System.out.println(numDigits);
        return totalTime;
    }

    private static BigInteger randomInt(Random random){
        char[] values = new char[random.nextInt(maxSizeForNumber)+1];
        for(int pos=0;pos<values.length;pos++){
            values[pos] = nums.charAt(random.nextInt(nums.length()));
        }

        return new BigInteger(new String(values));
    }
    */
}
