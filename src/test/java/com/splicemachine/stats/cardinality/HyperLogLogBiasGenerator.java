package com.splicemachine.stats.cardinality;

import com.splicemachine.hash.HashFunctions;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

/**
 * @author Scott Fines
 *         Date: 7/21/15
 */
public class HyperLogLogBiasGenerator{

    public static void main(String...args) throws Exception{
        int maxCardinality = 16;
//        int cardStep = 100000;
        int cardStep = maxCardinality;
        int p = 14;

        System.out.println("Expected sigma="+(1.04d/Math.sqrt(1<<p)));
        File dest = new File("target/bias.out");

//        int mBs = Math.max(100,maxCardinality/4);
        int mBs = maxCardinality/4;
        System.out.println(mBs);
        try(BufferedWriter writer = new BufferedWriter(new FileWriter(dest))){
            BaseLogLogCounter count = new SparseHyperLogLog(p,
                    HashFunctions.murmur2_64(0),
                    HyperLogLogBiasEstimators.biasEstimate(p),
                    mBs,mBs);
            BaseLogLogCounter denseCount = new AdjustedHyperLogLogCounter(p,HashFunctions.murmur2_64(0));
            for(int i=cardStep;i<=maxCardinality;i+=cardStep){
                for(int j=0;j<i;j++){
                    count.update(j);
                    denseCount.update(j);
                }
//                byte[] sparseRegister = ((SparseHyperLogLog)count).denseRegisters;
//                if(sparseRegister!=null){
//                    byte[] denseRegister=((AdjustedHyperLogLogCounter)denseCount).buckets;
//                    for(int k=0;k<sparseRegister.length;k++){
//                        if(sparseRegister[k]!=denseRegister[k])
//                            System.out.println(k);
//                    }
//                }
                double sparseRelErr = (double)(Math.abs(count.getEstimate()-i))/i;
                double denseRelErr = (double)(Math.abs(denseCount.getEstimate()-i))/i;
                System.out.printf("%d,%f,%f%n",i,sparseRelErr,denseRelErr);
                writer.write(i+","+sparseRelErr);
                writer.newLine();
            }
            writer.flush();
        }
    }
}
