package com.splicemachine.async;

public class GatheringScannerTest {

//    public static void main(String...args) throws Exception{
//        byte[] table = Bytes.toBytes(Long.toString(1184));
//        Scan baseScan = new Scan();
//        try{
//            AsyncScanner scanner = GatheringScanner.newScanner(table,baseScan,1<<16);
//            int count = 0;
//            Result r;
//            while((r = scanner.next())!=null){
//                count++;
//            }
//            System.out.println(count);
//        }finally{
//            HBaseRegionCache.getInstance().shutdown();
//            SimpleAsyncScanner.HBASE_CLIENT.shutdown().join();
//        }
//    }

}