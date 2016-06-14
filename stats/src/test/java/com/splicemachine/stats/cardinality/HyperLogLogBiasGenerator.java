package com.splicemachine.stats.cardinality;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.primitives.Longs;
import com.splicemachine.hash.Hash64;
import com.splicemachine.hash.HashFunctions;
import com.splicemachine.primitives.BigEndianBits;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author Scott Fines
 *         Date: 7/21/15
 */
public class HyperLogLogBiasGenerator{

    public static void main(String...args) throws Exception{
        generateBias();
    }

    private static void generateBias() throws Exception{
        int shift = 21;
        long maxCardinality = 1l<<shift;
//        long maxCardinality=33;
//        int cardStep = 1;
        int p = 14;

        System.out.println("Expected sigma="+(1.04d/Math.sqrt(1<<p)));
        File dest = new File("target/bias.out");

        Hash64 hashFunction=HashFunctions.murmur2_64(0);
//        Hash64 hashFunction= new GoogleHasher();
        ExecutorService executor =Executors.newFixedThreadPool(4);
        List<Future<String>> futures = new ArrayList<>(1);
        for(long i = maxCardinality;i>0;i>>>=1){
            futures.add(executor.submit(new Counter(i,hashFunction,p)));
        }

        try(BufferedWriter writer = new BufferedWriter(new FileWriter(dest))){
            for(int i=futures.size()-1;i>=0;i--){
                Future<String> future = futures.get(i);
                String value=future.get();
                writer.write(value);
                writer.newLine();
            }
            writer.flush();
        }finally{
            executor.shutdown();
        }
    }

    private static class Counter implements Callable<String>{
        private final long cardinality;
        private final Hash64 hf;
        private final int p;

        public Counter(long cardinality,Hash64 hf,int p){
            this.cardinality=cardinality;
            this.hf=hf;
            this.p=p;
        }

        @Override
        public String call() throws Exception{
            BaseLogLogCounter count = new SparseHyperLogLog(p, hf, HyperLogLogBiasEstimators.biasEstimate(p));
            BaseLogLogCounter denseCount = new AdjustedHyperLogLogCounter(p,hf);
            for(long j=1;j<=cardinality;j++){
                count.update(j);
                denseCount.update(j);
            }
            double sparseRelErr = (double)((count.getEstimate()-cardinality))/cardinality;
            double denseRelErr = (double)((denseCount.getEstimate()-cardinality))/cardinality;
            System.out.printf("[%s] %d,%d,%f,%f%n",Thread.currentThread().getName(),cardinality,count.getEstimate(),sparseRelErr,denseRelErr);
            return String.format("%d,%f",cardinality,sparseRelErr);
        }
    }

}
