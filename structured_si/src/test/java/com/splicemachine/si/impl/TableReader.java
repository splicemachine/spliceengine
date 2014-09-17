package com.splicemachine.si.impl;

import com.splicemachine.hbase.async.SimpleAsyncScanner;
import com.stumbleupon.async.Deferred;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * @author Scott Fines
 *         Date: 9/17/14
 */
public class TableReader {

    public static void main(String...args) throws Exception{
        int minStamp = 330;
        int maxTimestamp = 350;
        int numTxns = maxTimestamp - minStamp;
        final long[] count = new long[numTxns];
        boolean[] rolledBackTxns = new boolean[numTxns];
        Arrays.fill(rolledBackTxns,false);
        rolledBackTxns[331-minStamp] = true;
        rolledBackTxns[333-minStamp] = true;
        rolledBackTxns[334-minStamp] = true;
        rolledBackTxns[335-minStamp] = true;

        Scanner scanner = SimpleAsyncScanner.HBASE_CLIENT.newScanner("1472");
        try{
            scanner.setTimeRange(minStamp, maxTimestamp);
            scanner.setServerBlockCache(false);


            Deferred<ArrayList<ArrayList<KeyValue>>> deferred = scanner.nextRows();
            long rowCount = 0;
            ArrayList<ArrayList<KeyValue>> data;
            while((data = deferred.join())!=null){
                for(ArrayList<KeyValue> kvList: data){
                    rowCount++;
                    for(KeyValue kv:kvList){
                        int countPos = (int)(kv.timestamp()-minStamp);
                        count[countPos]++;
                    }
                    if(rowCount%1000==0)
                        System.out.printf("Visited %d rows%n", rowCount);
                }
                deferred = scanner.nextRows();
            }
            int totalCount=0;
            int nonRolledBackCount=0;
            for(int i=0;i<count.length;i++){
                int pos = i+minStamp;
                System.out.printf("count[%d]: %d%n",pos,count[i]);
                totalCount+=count[i];
                if(!rolledBackTxns[i])
                    nonRolledBackCount+=count[i];
            }
            System.out.printf("total count: %d%n",totalCount);
            System.out.printf("committed count: %d%n",nonRolledBackCount);

        }finally{
            SimpleAsyncScanner.HBASE_CLIENT.shutdown();
        }
    }
}
